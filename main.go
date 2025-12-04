// main.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	BinanceWSURL       = "wss://fstream.binance.com/ws"
	BatchSize          = 100 // Smaller batches for stability
	BatchPause         = 500 * time.Millisecond
	RawBuffer          = 20000
	QueueWarnEvery     = 3 * time.Second
	ReadDeadline       = 90 * time.Second
	PingInterval       = 3 * time.Minute
	ReconnectBaseDelay = 2 * time.Second
	ReconnectMaxDelay  = 30 * time.Second

	// Redis configuration
	RedisAddr       = "localhost:6379"
	RedisPassword   = ""
	RedisDB         = 0
	RedisChannel    = "book_tickers" // Channel for normalized data
	RedisMaxRetries = 3
	RedisPoolSize   = 10
)

var (
	dropCounter    int64
	publishCounter int64
	redisErrCount  int64
)

type RawWSMessage struct {
	Data []byte
}

type BookTicker struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidQty    string `json:"B"`
	AskPrice  string `json:"a"`
	AskQty    string `json:"A"`
	TransTs   int64  `json:"T"`
}

// NormalizedBookTicker is the unified format for all exchanges
type NormalizedBookTicker struct {
	Exchange      string  `json:"exchange"`
	Symbol        string  `json:"symbol"`
	BidPrice      float64 `json:"bid_price"`
	BidQty        float64 `json:"bid_qty"`
	AskPrice      float64 `json:"ask_price"`
	AskQty        float64 `json:"ask_qty"`
	Timestamp     int64   `json:"timestamp"`      // Unix milliseconds
	ReceivedAt    int64   `json:"received_at"`    // Unix milliseconds when we received it
	OriginalEvent string  `json:"original_event"` // For debugging
}

type ControlMsg struct {
	Result interface{} `json:"result"`
	ID     int64       `json:"id"`
	Error  *struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error"`
}

type RedisPublisher struct {
	client  *redis.Client
	channel string
	mu      sync.Mutex
}

func NewRedisPublisher(addr, password, channel string, db int) (*RedisPublisher, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		MaxRetries:   RedisMaxRetries,
		PoolSize:     RedisPoolSize,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	log.Printf("Redis connected successfully to %s", addr)
	return &RedisPublisher{
		client:  client,
		channel: channel,
	}, nil
}

func (rp *RedisPublisher) Publish(ctx context.Context, data *NormalizedBookTicker) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	if err := rp.client.Publish(ctx, rp.channel, payload).Err(); err != nil {
		atomic.AddInt64(&redisErrCount, 1)
		return fmt.Errorf("redis publish error: %w", err)
	}

	atomic.AddInt64(&publishCounter, 1)
	return nil
}

func (rp *RedisPublisher) Close() error {
	return rp.client.Close()
}

func main() {
	log.Println("starting binance -> redis publisher")

	// Initialize Redis publisher
	publisher, err := NewRedisPublisher(RedisAddr, RedisPassword, RedisChannel, RedisDB)
	if err != nil {
		log.Fatalf("failed to initialize redis: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawChan := make(chan RawWSMessage, RawBuffer)
	var wg sync.WaitGroup

	// Start workers with Redis publisher
	startWorkers(ctx, rawChan, publisher, &wg)
	go monitorQueue(rawChan)
	go printStats()

	// Managed connection: read, ping, reconnect
	go manageConnection(ctx, rawChan)

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutdown signal received")
	cancel()
	wg.Wait()
	log.Printf("FINAL STATS: dropped=%d published=%d redis_errors=%d",
		atomic.LoadInt64(&dropCounter),
		atomic.LoadInt64(&publishCounter),
		atomic.LoadInt64(&redisErrCount))
	log.Println("exited")
}

func printStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastPublished := int64(0)

	for range ticker.C {
		published := atomic.LoadInt64(&publishCounter)
		dropped := atomic.LoadInt64(&dropCounter)
		redisErrs := atomic.LoadInt64(&redisErrCount)
		rate := (published - lastPublished) / 10

		log.Printf("STATS: published=%d (%.1f/s) dropped=%d redis_errors=%d",
			published, float64(rate), dropped, redisErrs)
		lastPublished = published
	}
}

// manageConnection keeps the websocket alive with auto-reconnect and heartbeat
func manageConnection(ctx context.Context, rawChan chan<- RawWSMessage) {
	delay := ReconnectBaseDelay
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := connectAndSubscribe()
		if err != nil {
			log.Printf("connect/subscribe failed: %v", err)
			time.Sleep(delay)
			delay *= 2
			if delay > ReconnectMaxDelay {
				delay = ReconnectMaxDelay
			}
			continue
		}
		log.Println("connection established")
		delay = ReconnectBaseDelay

		// ping ticker
		pingTicker := time.NewTicker(PingInterval)
		defer pingTicker.Stop()

		readErrChan := make(chan error, 1)

		// read loop
		go func() {
			readErrChan <- readLoop(ctx, conn, rawChan)
		}()

	loop:
		for {
			select {
			case <-ctx.Done():
				log.Println("context canceled, closing connection")
				conn.Close()
				return
			case <-pingTicker.C:
				payload := []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))
				deadline := time.Now().Add(5 * time.Second)
				if err := conn.WriteControl(websocket.PingMessage, payload, deadline); err != nil {
					log.Printf("ping error: %v", err)
					conn.Close()
					break loop
				}
			case err := <-readErrChan:
				log.Printf("readLoop exited: %v", err)
				conn.Close()
				break loop
			}
		}
		log.Printf("reconnecting in %s...", delay)
		time.Sleep(delay)
		delay *= 2
		if delay > ReconnectMaxDelay {
			delay = ReconnectMaxDelay
		}
	}
}

// connectAndSubscribe establishes a websocket connection and subscribes to streams
func connectAndSubscribe() (*websocket.Conn, error) {
	d := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   8192,
		WriteBufferSize:  8192,
	}
	conn, _, err := d.Dial(BinanceWSURL, nil)
	if err != nil {
		return nil, err
	}

	// Set handlers before subscribing
	conn.SetReadDeadline(time.Now().Add(ReadDeadline))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(ReadDeadline))
		return nil
	})
	conn.SetPingHandler(func(msg string) error {
		deadline := time.Now().Add(5 * time.Second)
		return conn.WriteControl(websocket.PongMessage, []byte(msg), deadline)
	})

	symbolsList, err := symbols()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to fetch symbols: %w", err)
	}

	log.Printf("fetched %d symbols from API, subscribing to all...", len(symbolsList))

	symbolsLower := make([]string, len(symbolsList))
	for i, s := range symbolsList {
		symbolsLower[i] = strings.ToLower(s)
	}

	params := make([]string, 0, len(symbolsLower))
	for _, s := range symbolsLower {
		params = append(params, s+"@bookTicker")
	}

	log.Printf("subscribing to %d streams in batches of %d...", len(params), BatchSize)
	if err := subscribeInBatches(conn, params); err != nil {
		conn.Close()
		return nil, err
	}

	// Wait a bit for subscriptions to settle
	time.Sleep(1 * time.Second)
	log.Printf("subscription complete for all %d streams, waiting for data...", len(params))

	return conn, nil
}

func subscribeInBatches(conn *websocket.Conn, params []string) error {
	batchCount := 0
	for i := 0; i < len(params); i += BatchSize {
		end := i + BatchSize
		if end > len(params) {
			end = len(params)
		}
		batch := params[i:end]

		msg := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": batch,
			"id":     time.Now().UnixNano(),
		}

		// Set write deadline
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		if err := conn.WriteJSON(msg); err != nil {
			log.Printf("ERROR sending subscribe batch %d: %v", batchCount, err)
			return fmt.Errorf("failed to send subscribe batch: %w", err)
		}

		batchCount++
		log.Printf("subscribe batch %d sent (%d streams, total: %d/%d)",
			batchCount, len(batch), end, len(params))

		// Wait between batches to avoid overwhelming the server
		if i+BatchSize < len(params) {
			time.Sleep(BatchPause)
		}
	}

	log.Printf("all %d batches sent successfully", batchCount)
	return nil
}

// readLoop reads messages and pushes to rawChan
func readLoop(ctx context.Context, conn *websocket.Conn, rawChan chan<- RawWSMessage) error {
	msgCount := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(ReadDeadline))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure) {
				return fmt.Errorf("connection closed: %w", err)
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("connection closed")
			}
			if strings.Contains(err.Error(), "EOF") {
				return fmt.Errorf("unexpected EOF - connection dropped by server")
			}
			if isTemporaryNetErr(err) {
				log.Printf("temporary network error, retrying: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		msgCount++
		if msgCount == 1 {
			log.Printf("first message received, data flow started")
		}

		if looksLikeControlMessage(data) {
			logControlMessage(data)
			continue
		}

		select {
		case rawChan <- RawWSMessage{Data: data}:
		default:
			atomic.AddInt64(&dropCounter, 1)
		}
	}
}

func isTemporaryNetErr(err error) bool {
	if nerr, ok := err.(net.Error); ok {
		return nerr.Temporary() || nerr.Timeout()
	}
	return false
}

func looksLikeControlMessage(b []byte) bool {
	return len(b) > 0 && (bytes.Contains(b, []byte(`"result"`)) || bytes.Contains(b, []byte(`"error"`)))
}

func logControlMessage(b []byte) {
	var m ControlMsg
	if err := json.Unmarshal(b, &m); err != nil {
		log.Printf("control message (unparseable): %s", string(b))
		return
	}
	if m.Error != nil {
		log.Printf("CONTROL ERROR: code=%d msg=%s", m.Error.Code, m.Error.Msg)
		return
	}
	log.Printf("CONTROL OK: id=%d result=%v", m.ID, m.Result)
}

func startWorkers(ctx context.Context, rawChan <-chan RawWSMessage, publisher *RedisPublisher, wg *sync.WaitGroup) {
	WorkerCount := runtime.NumCPU()
	wg.Add(WorkerCount)
	for i := 0; i < WorkerCount; i++ {
		go func(id int) {
			defer wg.Done()
			log.Printf("worker %d started", id)
			for {
				select {
				case <-ctx.Done():
					log.Printf("worker %d exiting (ctx canceled)", id)
					return
				case raw, ok := <-rawChan:
					if !ok {
						log.Printf("worker %d exiting (channel closed)", id)
						return
					}
					processAndPublish(ctx, raw.Data, publisher)
				}
			}
		}(i)
	}
}

func processAndPublish(ctx context.Context, b []byte, publisher *RedisPublisher) {
	var bt BookTicker
	if err := json.Unmarshal(b, &bt); err != nil {
		return
	}

	// Normalize the data
	normalized, err := normalizeBookTicker(&bt)
	if err != nil {
		log.Printf("normalization error for %s: %v", bt.Symbol, err)
		return
	}

	// Publish to Redis
	if err := publisher.Publish(ctx, normalized); err != nil {
		log.Printf("redis publish error: %v", err)
	}
}

func normalizeBookTicker(bt *BookTicker) (*NormalizedBookTicker, error) {
	bidPrice, err := strconv.ParseFloat(bt.BidPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid bid price: %w", err)
	}

	bidQty, err := strconv.ParseFloat(bt.BidQty, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid bid qty: %w", err)
	}

	askPrice, err := strconv.ParseFloat(bt.AskPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ask price: %w", err)
	}

	askQty, err := strconv.ParseFloat(bt.AskQty, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ask qty: %w", err)
	}

	timestamp := bt.EventTime
	if timestamp == 0 && bt.TransTs > 0 {
		timestamp = bt.TransTs
	}

	return &NormalizedBookTicker{
		Exchange:      "binance",
		Symbol:        bt.Symbol,
		BidPrice:      bidPrice,
		BidQty:        bidQty,
		AskPrice:      askPrice,
		AskQty:        askQty,
		Timestamp:     timestamp,
		ReceivedAt:    time.Now().UnixMilli(),
		OriginalEvent: bt.EventType,
	}, nil
}

func monitorQueue(ch chan RawWSMessage) {
	t := time.NewTicker(QueueWarnEvery)
	defer t.Stop()
	for range t.C {
		n := len(ch)
		if n > RawBuffer/2 {
			log.Printf("WARNING: queue usage high: %d/%d", n, RawBuffer)
		}
	}
}

// symbols fetches symbols from external API
func symbols() ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://152.70.55.44:8080/intersection/futures")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	list := make([]string, len(apiResp.Symbols))
	for i, s := range apiResp.Symbols {
		list[i] = s.Symbol
	}
	return list, nil
}
