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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	BinanceWSURL   = "wss://fstream.binance.com/ws"
	BatchSize      = 180
	BatchPause     = 300 * time.Millisecond
	RawBuffer      = 20000
	QueueWarnEvery = 3 * time.Second
	ReadDeadline   = 90 * time.Second
	PingInterval   = 3 * time.Minute

	ReconnectBaseDelay = 1 * time.Second
	ReconnectMaxDelay  = 30 * time.Second
)

var dropCounter int64

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

type ControlMsg struct {
	Result interface{} `json:"result"`
	ID     int64       `json:"id"`
	Error  *struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error"`
}

func main() {
	log.Println("starting binance client")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawChan := make(chan RawWSMessage, RawBuffer)
	var wg sync.WaitGroup

	// start workers
	startWorkers(ctx, rawChan, &wg)
	go monitorQueue(rawChan)

	// managed connection: read, ping, reconnect
	go manageConnection(ctx, rawChan)

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutdown signal received")
	cancel()

	wg.Wait()
	log.Printf("FINAL STATS: dropped=%d", atomic.LoadInt64(&dropCounter))
	log.Println("exited")
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
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := d.Dial(BinanceWSURL, nil)
	if err != nil {
		return nil, err
	}

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
	symbolsLower := make([]string, len(symbolsList))
	for i, s := range symbolsList {
		symbolsLower[i] = strings.ToLower(s)
	}

	params := make([]string, 0, len(symbolsLower))
	for _, s := range symbolsLower {
		params = append(params, s+"@bookTicker")
	}

	if err := subscribeInBatches(conn, params); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func subscribeInBatches(conn *websocket.Conn, params []string) error {
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
		if err := conn.WriteJSON(msg); err != nil {
			log.Printf("ERROR sending subscribe batch: %v", err)
			return fmt.Errorf("failed to send subscribe batch: %w", err)
		}
		log.Printf("subscribe batch sent (%d streams)", len(batch))
		time.Sleep(BatchPause)
	}
	return nil
}

// readLoop reads messages and pushes to rawChan
func readLoop(ctx context.Context, conn *websocket.Conn, rawChan chan<- RawWSMessage) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(ReadDeadline))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
				strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("connection closed")
			}
			if isTemporaryNetErr(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("read error: %w", err)
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

func startWorkers(ctx context.Context, rawChan <-chan RawWSMessage, wg *sync.WaitGroup) {
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
					processRaw(raw.Data)
				}
			}
		}(i)
	}
}

func processRaw(b []byte) {
	var bt BookTicker
	if err := json.Unmarshal(b, &bt); err != nil {
		return
	}
	ts := time.Now()
	if bt.EventTime > 0 {
		ts = time.Unix(0, bt.EventTime*int64(time.Millisecond))
	}
	log.Printf("%s | bid=%s qty=%s | ask=%s qty=%s | %s",
		bt.Symbol, bt.BidPrice, bt.BidQty, bt.AskPrice, bt.AskQty, ts.Format("15:04:05.000"))
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
