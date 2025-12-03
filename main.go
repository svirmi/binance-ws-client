package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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

// BinanceFuturesWebSocketClient for orderbook level 1 data
type BinanceFuturesWebSocketClient struct {
	conn        *websocket.Conn
	url         string
	symbols     []string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	isRunning   atomic.Bool
	msgCounter  atomic.Int64 // Counts successfully processed messages
	dropCounter atomic.Int64 // [NEW] Counts messages dropped due to full buffer

	// [NEW] Buffer for messages. Decouples reading from processing.
	msgChan chan []byte
}

// BookTickerData represents the orderbook level 1 data from Binance
type BookTickerData struct {
	EventType string `json:"e"`
	UpdateID  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidQty    string `json:"B"`
	AskPrice  string `json:"a"`
	AskQty    string `json:"A"`
	EventTs   int64  `json:"E"`
	TransTs   int64  `json:"T"`
}

// NewBinanceFuturesWebSocketClient creates a new client
func NewBinanceFuturesWebSocketClient(symbols []string) *BinanceFuturesWebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	lowerSymbols := make([]string, len(symbols))
	for i, symbol := range symbols {
		lowerSymbols[i] = strings.ToLower(symbol)
	}

	return &BinanceFuturesWebSocketClient{
		url:         "wss://fstream.binance.com/ws",
		symbols:     lowerSymbols,
		ctx:         ctx,
		cancel:      cancel,
		isRunning:   atomic.Bool{},
		msgCounter:  atomic.Int64{},
		dropCounter: atomic.Int64{},
		// [MODIFIED] Increased buffer size to 10,000 to better handle high-frequency bursts
		msgChan: make(chan []byte, 10000),
	}
}

// Start kicks off the main run loop
func (c *BinanceFuturesWebSocketClient) Start() error {
	if !c.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("client already running")
	}

	c.wg.Add(1)
	go c.run()

	// [MODIFIED] Worker Pool: Start multiple workers to process messages in parallel.
	// This significantly increases throughput (JSON parsing is CPU bound).
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		c.wg.Add(1)
		go c.workerLoop(i)
	}

	return nil
}

// [MODIFIED] workerLoop consumes messages from the buffer and processes them.
func (c *BinanceFuturesWebSocketClient) workerLoop(id int) {
	defer c.wg.Done()
	// log.Printf("Worker %d started", id) // Optional debug

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.msgChan:
			// Actual heavy lifting (JSON parsing, logging) happens here
			c.processMessage(msg)
			c.msgCounter.Add(1)
		}
	}
}

func (c *BinanceFuturesWebSocketClient) run() {
	defer c.wg.Done()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for c.isRunning.Load() {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		conn, err := c.connect()
		if err != nil {
			log.Printf("[run] Failed to connect: %v", err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = time.Second
		c.conn = conn
		log.Println("[run] Connected and subscribed successfully")

		connCtx, connCancel := context.WithCancel(c.ctx)
		var connWG sync.WaitGroup
		connWG.Add(2)
		go c.readLoop(connCtx, connCancel, &connWG)
		go c.heartbeatLoop(connCtx, connCancel, &connWG)

		connWG.Wait()
		connCancel()

		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}

		if !c.isRunning.Load() || c.ctx.Err() != nil {
			return
		}

		log.Println("[run] Attempting to reconnect...")
	}
}

func (c *BinanceFuturesWebSocketClient) connect() (*websocket.Conn, error) {
	log.Printf("[connect] Starting Binance Futures WebSocket client for %d symbols", len(c.symbols))

	conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Binance: %w", err)
	}

	readTimeout := 90 * time.Second
	conn.SetReadDeadline(time.Now().Add(readTimeout))

	conn.SetPongHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	conn.SetPingHandler(func(appData string) error {
		deadline := time.Now().Add(5 * time.Second)
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), deadline)
		if err != nil {
			log.Printf("[ping-handler] Failed to send pong: %v", err)
		} else {
			_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		}
		return nil
	})

	params := make([]string, 0, len(c.symbols))
	for _, symbol := range c.symbols {
		stream := fmt.Sprintf("%s@bookTicker", symbol)
		params = append(params, stream)
	}

	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     1,
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	return conn, nil
}

func (c *BinanceFuturesWebSocketClient) readLoop(connCtx context.Context, connCancel context.CancelFunc, connWG *sync.WaitGroup) {
	defer connWG.Done()

	for {
		select {
		case <-connCtx.Done():
			return
		default:
		}

		if c.conn == nil {
			return
		}

		c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		_, message, err := c.conn.ReadMessage()
		if err != nil {
			// Suppress "use of closed network connection" error on shutdown
			if !c.isRunning.Load() && strings.Contains(err.Error(), "use of closed network connection") {
				return
			}

			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("[readLoop] WebSocket read error: %v", err)
			} else {
				log.Printf("[readLoop] WebSocket read error (non-unexpected): %v", err)
			}
			connCancel()
			return
		}

		select {
		case c.msgChan <- message:
			// Message successfully buffered
		default:
			// Channel is full (worker is too slow)
			c.dropCounter.Add(1)
		}
	}
}

func (c *BinanceFuturesWebSocketClient) heartbeatLoop(connCtx context.Context, connCancel context.CancelFunc, connWG *sync.WaitGroup) {
	defer connWG.Done()

	interval := 3 * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-connCtx.Done():
			return
		case <-ticker.C:
			if c.conn == nil {
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			payload := []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))
			if err := c.conn.WriteControl(websocket.PingMessage, payload, deadline); err != nil {
				log.Printf("[heartbeatLoop] Failed to send ping control frame: %v", err)
				connCancel()
				return
			}
		}
	}
}

func (c *BinanceFuturesWebSocketClient) processMessage(data []byte) {
	var bookTicker BookTickerData
	if err := json.Unmarshal(data, &bookTicker); err == nil {
		if bookTicker.EventType == "bookTicker" && bookTicker.Symbol != "" && bookTicker.BidPrice != "" && bookTicker.AskPrice != "" {
			c.logBookTicker(bookTicker)
			return
		}
	}

	var combinedMsg struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &combinedMsg); err == nil && combinedMsg.Stream != "" {
		if strings.Contains(combinedMsg.Stream, "@bookTicker") {
			var ticker BookTickerData
			if err := json.Unmarshal(combinedMsg.Data, &ticker); err == nil {
				c.logBookTicker(ticker)
			}
		}
		return
	}

	var controlMsg map[string]interface{}
	if err := json.Unmarshal(data, &controlMsg); err == nil {
		if _, ok := controlMsg["result"]; ok {
			return
		}
		if errVal, ok := controlMsg["error"]; ok {
			log.Printf("[processMessage] Received error from Binance: %v", errVal)
			return
		}
	}
}

func (c *BinanceFuturesWebSocketClient) logBookTicker(ticker BookTickerData) {
	var ts time.Time
	// Removed artificial sleep for production performance

	if ticker.EventTs > 0 {
		ts = time.Unix(0, ticker.EventTs*int64(time.Millisecond))
	} else if ticker.TransTs > 0 {
		ts = time.Unix(0, ticker.TransTs*int64(time.Millisecond))
	} else {
		ts = time.Now()
	}

	log.Printf("%s | Bid: %s/%s | Ask: %s/%s | %s",
		strings.ToUpper(ticker.Symbol),
		ticker.BidPrice, ticker.BidQty,
		ticker.AskPrice, ticker.AskQty,
		ts.Format("15:04:05.000"))
}

func (c *BinanceFuturesWebSocketClient) Stop() {
	log.Println("Stopping Binance Futures WebSocket client...")

	if !c.isRunning.CompareAndSwap(true, false) {
		return
	}

	c.cancel()

	if c.conn != nil {
		_ = c.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		_ = c.conn.Close()
	}

	c.wg.Wait()

	processed := c.msgCounter.Load()
	dropped := c.dropCounter.Load()
	log.Printf("Client stopped. Processed: %d, Dropped: %d", processed, dropped)
}

func main() {
	// rand.Seed is deprecated in newer Go versions, but kept for compatibility
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
	}

	log.Printf("Starting Orderbook Level 1 Data Collector for %d symbols", len(symbols))

	client := NewBinanceFuturesWebSocketClient(symbols)

	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-statsTicker.C:
				if client.isRunning.Load() {
					processed := client.msgCounter.Load()
					dropped := client.dropCounter.Load()
					pending := len(client.msgChan)
					cap := cap(client.msgChan)

					log.Printf("STATS: Processed: %d | Dropped: %d | Buffer: %d/%d",
						processed, dropped, pending, cap)
				}
			case <-client.ctx.Done():
				return
			}
		}
	}()

	if err := client.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("Press Ctrl+C to stop...")

	<-sigChan
	log.Println("\nShutdown signal received")

	client.Stop()

	log.Println("Orderbook data collector stopped")
}
