// File: main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ------------------------------------------------------------------
//
//	Normalized Quote Struct (common format for all exchanges)
//
// ------------------------------------------------------------------
type NormalizedQuote struct {
	Exchange  string    `json:"exchange"`
	Symbol    string    `json:"symbol"`
	BidPrice  float64   `json:"bid_price"`
	BidSize   float64   `json:"bid_size"`
	AskPrice  float64   `json:"ask_price"`
	AskSize   float64   `json:"ask_size"`
	Timestamp time.Time `json:"timestamp"`
}

// ------------------------------------------------------------------
// Binance Raw BookTicker Message
// ------------------------------------------------------------------
type BinanceBookTicker struct {
	Symbol   string `json:"s"`
	BidPrice string `json:"b"`
	BidQty   string `json:"B"`
	AskPrice string `json:"a"`
	AskQty   string `json:"A"`
	Time     int64  `json:"T"`
}

// ------------------------------------------------------------------
// Normalizer for Binance → NormalizedQuote
// ------------------------------------------------------------------
func NormalizeBinanceBookTicker(msg BinanceBookTicker) (NormalizedQuote, error) {
	var nq NormalizedQuote
	nq.Exchange = "binance"
	nq.Symbol = msg.Symbol

	var err error
	if nq.BidPrice, err = parseFloat(msg.BidPrice); err != nil {
		return nq, fmt.Errorf("invalid bid price: %w", err)
	}
	if nq.AskPrice, err = parseFloat(msg.AskPrice); err != nil {
		return nq, fmt.Errorf("invalid ask price: %w", err)
	}
	if nq.BidSize, err = parseFloat(msg.BidQty); err != nil {
		return nq, fmt.Errorf("invalid bid qty: %w", err)
	}
	if nq.AskSize, err = parseFloat(msg.AskQty); err != nil {
		return nq, fmt.Errorf("invalid ask qty: %w", err)
	}

	// Event time T is transaction time in ms; convert to time.Time safely
	if msg.Time > 0 {
		nq.Timestamp = time.UnixMilli(msg.Time)
	} else {
		nq.Timestamp = time.Now()
	}
	return nq, nil
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// ------------------------------------------------------------------
// Collector
// ------------------------------------------------------------------

type Collector struct {
	wsURL       string
	symbols     []string
	rawRead     int64
	processed   int64
	dropped     int64
	workerCount int
	msgCh       chan []byte
}

// ------------------------------------------------------------------

func NewCollector(wsURL string, symbols []string, workerCount int, buffer int) *Collector {
	return &Collector{
		wsURL:       wsURL,
		symbols:     symbols,
		workerCount: workerCount,
		msgCh:       make(chan []byte, buffer),
	}
}

// ------------------------------------------------------------------

func (c *Collector) connect() (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second,
		NetDialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).DialContext,
	}

	conn, _, err := dialer.Dial(c.wsURL, nil)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// ------------------------------------------------------------------
// Subscribe
// ------------------------------------------------------------------

func (c *Collector) subscribe(conn *websocket.Conn) error {
	type subReq struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int      `json:"id"`
	}

	var params []string
	for _, s := range c.symbols {
		// Binance expects lowercase stream names
		params = append(params, fmt.Sprintf("%s@bookTicker", strings.ToLower(s)))
	}

	req := subReq{
		Method: "SUBSCRIBE",
		Params: params,
		ID:     1,
	}

	if err := conn.WriteJSON(req); err != nil {
		return err
	}
	log.Printf("subscribe request sent for %d streams", len(params))
	return nil
}

// ------------------------------------------------------------------

func (c *Collector) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		// Respect ctx cancellation by checking before each read attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			// Return the error so caller (Run) can decide to reconnect/close
			return err
		}

		c.rawRead++

		// Drop detection (non-blocking send)
		select {
		case c.msgCh <- data:
		default:
			c.dropped++ // <---- keep counting drops
		}
	}
}

// ------------------------------------------------------------------
// Worker — now decodes raw → normalized
// ------------------------------------------------------------------

func (c *Collector) worker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("worker %d started", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d exiting (ctx canceled)", id)
			// drain remaining messages if any (optional)
			for {
				select {
				case msg := <-c.msgCh:
					var raw BinanceBookTicker
					if err := json.Unmarshal(msg, &raw); err == nil {
						if normalized, err := NormalizeBinanceBookTicker(raw); err == nil {
							_ = normalized // placeholder for later publishing
						}
					}
				default:
					return
				}
			}

		case msg, ok := <-c.msgCh:
			if !ok {
				log.Printf("worker %d exiting (channel closed)", id)
				return
			}
			var raw BinanceBookTicker
			if err := json.Unmarshal(msg, &raw); err != nil {
				continue
			}

			normalized, err := NormalizeBinanceBookTicker(raw)
			if err != nil {
				continue
			}

			// You will later send normalized to Redis
			// For now we log it to verify normalization is working
			log.Printf("normalized: %+v", normalized)

			c.processed++
		}
	}
}

// ------------------------------------------------------------------
// Run
// ------------------------------------------------------------------

func (c *Collector) Run(ctx context.Context) error {
	// Connect (with simple retry)
	var conn *websocket.Conn
	var err error
	for {
		conn, err = c.connect()
		if err == nil {
			break
		}
		log.Printf("connect failed: %v (retrying...)", err)
		time.Sleep(2 * time.Second)
	}
	log.Println("connected")

	// Subscribe
	if err := c.subscribe(conn); err != nil {
		conn.Close()
		return fmt.Errorf("subscribe failed: %w", err)
	}

	// Start workers (use WaitGroup so they can be waited on if needed)
	var wg sync.WaitGroup
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, i, &wg)
	}

	// Run readLoop in a goroutine so we can cancel/close connection on ctx cancel
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- c.readLoop(ctx, conn)
	}()

	// Wait for either read error or context cancellation
	select {
	case <-ctx.Done():
		// context canceled — close connection to unblock ReadMessage
		log.Println("Run: context canceled, closing websocket connection")
		conn.Close()
		// wait for readLoop to return
		err := <-readErrCh
		// close message channel to let workers drain and exit
		close(c.msgCh)
		// wait for workers
		wg.Wait()
		return ctx.ErrOr(err)
	case err := <-readErrCh:
		// readLoop returned (error or nil). make sure we close ws and exit.
		log.Printf("Run: readLoop returned: %v", err)
		conn.Close()
		// close msg channel and wait workers
		close(c.msgCh)
		wg.Wait()
		return err
	}
}

// ctx.ErrOr helper: prefer ctx.Err if not nil else return other error (small util)
func (ctx context.Context) ErrOr(other error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return other
}

// ------------------------------------------------------------------

func main() {
	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"} // example

	c := NewCollector(
		"wss://stream.binance.com:9443/ws",
		symbols,
		8,   // workers (set to runtime.NumCPU() if you prefer)
		500, // buffer
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutdown signal received")
		cancel()
	}()

	err := c.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("run error: %v", err)
	}

	log.Printf("FINAL STATS: raw=%d processed=%d dropped=%d",
		c.rawRead, c.processed, c.dropped)

	log.Println("exited")
}
