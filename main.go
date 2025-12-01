package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// BinanceFuturesWebSocketClient for orderbook level 1 data
type BinanceFuturesWebSocketClient struct {
	conn       *websocket.Conn
	url        string
	symbols    []string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	isRunning  atomic.Bool
	msgCounter atomic.Int64
}

// BookTickerData represents the orderbook level 1 data from Binance
type BookTickerData struct {
	EventType string `json:"e"` // Event type (should be "bookTicker")
	UpdateID  int64  `json:"u"` // Order book updateId
	Symbol    string `json:"s"` // Symbol
	BidPrice  string `json:"b"` // Best bid price
	BidQty    string `json:"B"` // Best bid quantity
	AskPrice  string `json:"a"` // Best ask price
	AskQty    string `json:"A"` // Best ask quantity
	EventTs   int64  `json:"E"` // Event time
	TransTs   int64  `json:"T"` // Transaction time
}

// NewBinanceFuturesWebSocketClient creates a new client
func NewBinanceFuturesWebSocketClient(symbols []string) *BinanceFuturesWebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	// Convert symbols to lowercase for WebSocket streams
	lowerSymbols := make([]string, len(symbols))
	for i, symbol := range symbols {
		lowerSymbols[i] = strings.ToLower(symbol)
	}

	return &BinanceFuturesWebSocketClient{
		url:        "wss://fstream.binance.com/ws",
		symbols:    lowerSymbols,
		ctx:        ctx,
		cancel:     cancel,
		isRunning:  atomic.Bool{},
		msgCounter: atomic.Int64{},
	}
}

// Start kicks off the main run loop that manages connection and reconnection.
func (c *BinanceFuturesWebSocketClient) Start() error {
	if !c.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("client already running")
	}

	c.wg.Add(1)
	go c.run()

	return nil
}

// run manages the connect → read/heartbeat → reconnect loop.
func (c *BinanceFuturesWebSocketClient) run() {
	defer c.wg.Done()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for c.isRunning.Load() {
		// Top-level context canceled? Then we exit the whole run loop.
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// --- Connect ---
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

		// Reset backoff on successful connect
		backoff = time.Second
		c.conn = conn
		log.Println("[run] Connected and subscribed successfully")

		// --- Per-connection context ---
		connCtx, connCancel := context.WithCancel(c.ctx)

		// Start loops tied to this connection
		var connWG sync.WaitGroup
		connWG.Add(2)
		go c.readLoop(connCtx, connCancel, &connWG)
		go c.heartbeatLoop(connCtx, connCancel, &connWG)

		// Wait until both loops finish (they call connCancel on error)
		connWG.Wait()
		connCancel()

		// Close this connection safely once loops are done
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

// connect dials Binance, sets handlers, and subscribes to bookTicker streams.
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
		// Uncomment if you want to see pongs:
		// log.Printf("[pong] %s", appData)
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

	// Subscribe to bookTicker streams
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

// readLoop reads and processes incoming messages for a single connection.
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
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("[readLoop] WebSocket read error: %v", err)
			} else {
				log.Printf("[readLoop] WebSocket read error (non-unexpected): %v", err)
			}
			// Trigger reconnect by canceling this connection context.
			connCancel()
			return
		}

		c.processMessage(message)
		c.msgCounter.Add(1)
	}
}

// heartbeatLoop sends periodic pings to keep connection alive.
func (c *BinanceFuturesWebSocketClient) heartbeatLoop(connCtx context.Context, connCancel context.CancelFunc, connWG *sync.WaitGroup) {
	defer connWG.Done()

	// Binance recommends ping/pong every 3 minutes if idle
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
				// Trigger reconnect
				connCancel()
				return
			}
		}
	}
}

// processMessage handles incoming WebSocket messages
func (c *BinanceFuturesWebSocketClient) processMessage(data []byte) {
	// Try to parse as book ticker event first (direct format)
	var bookTicker BookTickerData
	if err := json.Unmarshal(data, &bookTicker); err == nil {
		// Check if it's a bookTicker event and has required fields
		if bookTicker.EventType == "bookTicker" && bookTicker.Symbol != "" && bookTicker.BidPrice != "" && bookTicker.AskPrice != "" {
			c.logBookTicker(bookTicker)
			return
		}
	}

	// Try combined stream format (stream wrapper)
	var combinedMsg struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &combinedMsg); err == nil && combinedMsg.Stream != "" {
		// Check if it's a bookTicker stream
		if strings.Contains(combinedMsg.Stream, "@bookTicker") {
			var ticker BookTickerData
			if err := json.Unmarshal(combinedMsg.Data, &ticker); err == nil {
				c.logBookTicker(ticker)
			}
		}
		return
	}

	// Check if it's a control message (subscription response, error)
	var controlMsg map[string]interface{}
	if err := json.Unmarshal(data, &controlMsg); err == nil {
		if _, ok := controlMsg["result"]; ok {
			// Subscription confirmation, ignore
			return
		}
		if errVal, ok := controlMsg["error"]; ok {
			log.Printf("[processMessage] Received error from Binance: %v", errVal)
			return
		}
	}

	// Log unhandled messages for debugging (skip very small ones)
	if len(data) > 10 {
		log.Printf("[processMessage] DEBUG - Unhandled message structure: %s", string(data))
	}
}

// logBookTicker logs orderbook level 1 data
func (c *BinanceFuturesWebSocketClient) logBookTicker(ticker BookTickerData) {
	var ts time.Time
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

// Stop gracefully stops the client
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

	log.Printf("Client stopped. Total messages processed: %d", c.msgCounter.Load())
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
	}

	log.Printf("Starting Orderbook Level 1 Data Collector for %d symbols", len(symbols))

	// Create and start client
	client := NewBinanceFuturesWebSocketClient(symbols)

	// Start statistics reporter
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-statsTicker.C:
				if client.isRunning.Load() {
					log.Printf("STATS: Messages processed: %d", client.msgCounter.Load())
				}
			case <-client.ctx.Done():
				return
			}
		}
	}()

	// Start the client
	if err := client.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("Press Ctrl+C to stop...")

	<-sigChan
	log.Println("\nShutdown signal received")

	// Stop the client
	client.Stop()

	log.Println("Orderbook data collector stopped")
}
