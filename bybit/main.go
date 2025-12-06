// File: cmd/bybit2redis/main.go
//
// Bybit -> Redis publisher (Level 1 / best bid-ask)
// - Uses Bybit V5 linear public websocket: wss://stream.bybit.com/v5/public/linear
// - Subscribes to orderbook.1.<SYMBOL> for symbols returned by symbols() (same intersection URL + excluded list as your Binance program).
// - Normalizes best bid/ask to NormalizedBookTicker and publishes to Redis channel.
//
// Usage: compile and run. Change Redis constants as needed.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	BybitWSLinear      = "wss://stream.bybit.com/v5/public/linear"
	MaxSymbolsPerConn  = 40
	BatchSize          = 25
	BatchPause         = 300 * time.Millisecond
	RawBuffer          = 85000
	QueueWarnEvery     = 3 * time.Second
	ReadDeadline       = 60 * time.Second
	PingInterval       = 3 * time.Minute
	ReconnectBaseDelay = 2 * time.Second
	ReconnectMaxDelay  = 30 * time.Second

	RedisAddr       = "localhost:6379"
	RedisPassword   = ""
	RedisDB         = 0
	RedisChannel    = "bybit_orderbook"
	RedisMaxRetries = 3
	RedisPoolSize   = 10
)

var (
	dropCounter    int64
	publishCounter int64
	redisErrCount  int64
)

// SafeWebSocket wraps websocket.Conn for thread-safe writes
type SafeWebSocket struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (s *SafeWebSocket) WriteJSON(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(v)
}

func (s *SafeWebSocket) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteControl(messageType, data, deadline)
}

func (s *SafeWebSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

func (s *SafeWebSocket) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.SetWriteDeadline(t)
}

func (s *SafeWebSocket) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.SetReadDeadline(t)
}

func (s *SafeWebSocket) ReadMessage() (messageType int, p []byte, err error) {
	return s.conn.ReadMessage()
}

type RawWSMessage struct {
	Data []byte
}

// Bybit orderbook (level 1) message structure (we only map fields we need)
type BybitOrderbookMsg struct {
	Topic string `json:"topic"`
	Type  string `json:"type,omitempty"`
	TS    int64  `json:"ts,omitempty"`
	Data  struct {
		S   string     `json:"s"`
		B   [][]string `json:"b"`
		A   [][]string `json:"a"`
		U   int64      `json:"u,omitempty"`
		Seq int64      `json:"seq,omitempty"`
		CTS int64      `json:"cts,omitempty"`
	} `json:"data"`
	CTS int64 `json:"cts,omitempty"`
}

type NormalizedBookTicker struct {
	Exchange      string  `json:"exchange"`
	Symbol        string  `json:"symbol"`
	BidPrice      float64 `json:"bid_price"`
	BidQty        float64 `json:"bid_qty"`
	AskPrice      float64 `json:"ask_price"`
	AskQty        float64 `json:"ask_qty"`
	Timestamp     int64   `json:"timestamp"`
	ReceivedAt    int64   `json:"received_at"`
	OriginalTopic string  `json:"original_topic"`
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
	log.Println("starting bybit -> redis publisher (orderbook.1)")

	publisher, err := NewRedisPublisher(RedisAddr, RedisPassword, RedisChannel, RedisDB)
	if err != nil {
		log.Fatalf("failed to initialize redis: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use same symbols() source and excludedSymbols list as your Binance program
	allSymbols, err := symbols()
	if err != nil {
		log.Fatalf("failed to fetch symbols: %v", err)
	}
	log.Printf("fetched %d symbols total", len(allSymbols))

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allSymbols), func(i, j int) {
		allSymbols[i], allSymbols[j] = allSymbols[j], allSymbols[i]
	})
	log.Printf("symbols %d randomized for balanced distribution across shards", len(allSymbols))

	rawChan := make(chan RawWSMessage, RawBuffer)
	var wg sync.WaitGroup

	startWorkers(ctx, rawChan, publisher, &wg)
	go monitorQueue(rawChan)
	go printStats()

	chunkCount := 0
	for i := 0; i < len(allSymbols); i += MaxSymbolsPerConn {
		end := i + MaxSymbolsPerConn
		if end > len(allSymbols) {
			end = len(allSymbols)
		}
		chunk := allSymbols[i:end]
		chunkID := chunkCount
		chunkCount++

		log.Printf("[Main] Starting Shard %d with %d symbols", chunkID, len(chunk))
		go manageConnection(ctx, rawChan, chunk, chunkID)
		time.Sleep(200 * time.Millisecond)
	}

	log.Printf("All %d shards started", chunkCount)

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

func manageConnection(ctx context.Context, rawChan chan<- RawWSMessage, symbols []string, shardID int) {
	delay := ReconnectBaseDelay
	prefix := fmt.Sprintf("[Shard %d]", shardID)

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := dialBybit()
		if err != nil {
			log.Printf("%s dial failed: %v", prefix, err)
			time.Sleep(delay)
			delay *= 2
			if delay > ReconnectMaxDelay {
				delay = ReconnectMaxDelay
			}
			continue
		}

		log.Printf("%s connection established", prefix)
		delay = ReconnectBaseDelay

		readErrChan := make(chan error, 1)
		go func() {
			readErrChan <- readLoop(ctx, conn, rawChan, prefix)
		}()

		pingTicker := time.NewTicker(PingInterval)

		go func() {
			if err := fetchAndSubscribeBybit(conn, symbols, prefix); err != nil {
				log.Printf("%s subscription failed: %v", prefix, err)
				conn.Close()
			}
		}()

	loop:
		for {
			select {
			case <-ctx.Done():
				pingTicker.Stop()
				conn.Close()
				return
			case <-pingTicker.C:
				payload := []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))
				deadline := time.Now().Add(5 * time.Second)
				if err := conn.WriteControl(websocket.PingMessage, payload, deadline); err != nil {
					log.Printf("%s ping error: %v", prefix, err)
					pingTicker.Stop()
					conn.Close()
					break loop
				}
			case err := <-readErrChan:
				log.Printf("%s readLoop exited: %v", prefix, err)
				pingTicker.Stop()
				conn.Close()
				break loop
			}
		}

		log.Printf("%s reconnecting in %s...", prefix, delay)
		time.Sleep(delay)
		delay *= 2
		if delay > ReconnectMaxDelay {
			delay = ReconnectMaxDelay
		}
	}
}

func dialBybit() (*SafeWebSocket, error) {
	d := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   65536,
		WriteBufferSize:  8192,
	}

	rawConn, _, err := d.Dial(BybitWSLinear, nil)
	if err != nil {
		return nil, err
	}

	safeConn := &SafeWebSocket{conn: rawConn}

	_ = safeConn.SetReadDeadline(time.Now().Add(ReadDeadline))

	safeConn.conn.SetPongHandler(func(string) error {
		_ = safeConn.SetReadDeadline(time.Now().Add(ReadDeadline))
		return nil
	})

	safeConn.conn.SetPingHandler(func(msg string) error {
		deadline := time.Now().Add(5 * time.Second)
		return safeConn.WriteControl(websocket.PongMessage, []byte(msg), deadline)
	})

	return safeConn, nil
}

func fetchAndSubscribeBybit(conn *SafeWebSocket, symbolsList []string, prefix string) error {
	log.Printf("%s subscribing to %d symbols...", prefix, len(symbolsList))

	args := make([]string, 0, len(symbolsList))
	for _, s := range symbolsList {
		args = append(args, "orderbook.1."+s)
	}

	batchCount := 0
	for i := 0; i < len(args); i += BatchSize {
		end := i + BatchSize
		if end > len(args) {
			end = len(args)
		}
		batch := args[i:end]

		msg := map[string]interface{}{
			"op":   "subscribe",
			"args": batch,
		}

		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := conn.WriteJSON(msg); err != nil {
			return fmt.Errorf("failed to send subscribe batch: %w", err)
		}

		batchCount++
		if batchCount == 1 || i+BatchSize >= len(args) {
			log.Printf("%s sent batch %d (%d topics)", prefix, batchCount, len(batch))
		}

		if i+BatchSize < len(args) {
			time.Sleep(BatchPause)
		}
	}

	return nil
}

func readLoop(ctx context.Context, safeConn *SafeWebSocket, rawChan chan<- RawWSMessage, prefix string) error {
	msgCount := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
		}

		_ = safeConn.SetReadDeadline(time.Now().Add(ReadDeadline))
		_, data, err := safeConn.ReadMessage()
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
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		msgCount++
		if msgCount == 1 {
			log.Printf("%s first message received", prefix)
		}

		if looksLikeControlMessage(data) {
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
	return len(b) > 0 && (bytes.Contains(b, []byte(`"success"`)) || bytes.Contains(b, []byte(`"op"`)) && bytes.Contains(b, []byte(`"subscribe"`)))
}

func startWorkers(ctx context.Context, rawChan <-chan RawWSMessage, publisher *RedisPublisher, wg *sync.WaitGroup) {
	WorkerCount := runtime.NumCPU() * 2
	wg.Add(WorkerCount)

	for i := 0; i < WorkerCount; i++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case raw, ok := <-rawChan:
					if !ok {
						return
					}
					processAndPublish(ctx, raw.Data, publisher)
				}
			}
		}(i)
	}
}

func processAndPublish(ctx context.Context, b []byte, publisher *RedisPublisher) {
	var msg BybitOrderbookMsg
	if err := json.Unmarshal(b, &msg); err != nil {
		return
	}

	// require at least one bid and one ask
	if len(msg.Data.B) == 0 || len(msg.Data.A) == 0 {
		return
	}

	// best bid is first element in B, best ask is first element in A
	bestBid := msg.Data.B[0]
	bestAsk := msg.Data.A[0]
	if len(bestBid) < 2 || len(bestAsk) < 2 {
		return
	}

	bidPrice, err := strconv.ParseFloat(bestBid[0], 64)
	if err != nil {
		return
	}
	bidQty, err := strconv.ParseFloat(bestBid[1], 64)
	if err != nil {
		return
	}
	askPrice, err := strconv.ParseFloat(bestAsk[0], 64)
	if err != nil {
		return
	}
	askQty, err := strconv.ParseFloat(bestAsk[1], 64)
	if err != nil {
		return
	}

	timestamp := msg.TS
	if timestamp == 0 {
		if msg.Data.CTS > 0 {
			timestamp = msg.Data.CTS
		} else if msg.CTS > 0 {
			timestamp = msg.CTS
		}
	}

	normalized := &NormalizedBookTicker{
		Exchange:      "bybit",
		Symbol:        msg.Data.S,
		BidPrice:      bidPrice,
		BidQty:        bidQty,
		AskPrice:      askPrice,
		AskQty:        askQty,
		Timestamp:     timestamp,
		ReceivedAt:    time.Now().UnixMilli(),
		OriginalTopic: msg.Topic,
	}

	_ = publisher.Publish(ctx, normalized) // errors counted by publisher
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

// symbols fetches symbols from your intersection API and filters excludedSymbols (same list as your Binance program)
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

	excludedSymbols := map[string]struct{}{
		"ETHUSDT":  {},
		"SOLUSDT":  {},
		"BTCUSDT":  {},
		"XRPUSDT":  {},
		"BNBUSDT":  {},
		"DOGEUSDT": {},
		"LTCUSDT":  {},
		"ADAUSDT":  {},
		"ATOMUSDT": {},
		"SUIUSDT":  {},
		"HYPEUSDT": {},
		"LINKUSDT": {},
	}

	var list []string
	for _, s := range apiResp.Symbols {
		if _, isExcluded := excludedSymbols[s.Symbol]; !isExcluded {
			list = append(list, s.Symbol)
		}
	}

	return list, nil
}
