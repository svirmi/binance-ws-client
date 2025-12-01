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

// Start connects and starts receiving data
func (c *BinanceFuturesWebSocketClient) Start() error {
	log.Printf("Starting Binance Futures WebSocket client for %d symbols", len(c.symbols))

	// Create connection
	conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Binance: %w", err)
	}
	c.conn = conn

	// Set initial read deadline and handlers
	// Choose a generous read timeout; we'll extend it in the pong handler.
	readTimeout := 90 * time.Second
	c.conn.SetReadDeadline(time.Now().Add(readTimeout))

	// Pong handler extends deadline
	c.conn.SetPongHandler(func(appData string) error {
		// extend read deadline on every pong
		_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))
		log.Printf("Received pong (extend read deadline): %s", appData)
		return nil
	})

	// Ping handler: respond with a pong control frame
	c.conn.SetPingHandler(func(appData string) error {
		// write pong control frame in reply to server ping
		deadline := time.Now().Add(5 * time.Second)
		err := c.conn.WriteControl(websocket.PongMessage, []byte(appData), deadline)
		if err != nil {
			log.Printf("Failed to send pong in PingHandler: %v", err)
		} else {
			// also extend read deadline locally to avoid read timeout
			_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))
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

	if err := c.conn.WriteJSON(subscribeMsg); err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	c.isRunning.Store(true)

	// Start reader
	c.wg.Add(1)
	go c.readMessages()

	log.Println("Connected and subscribed successfully")
	return nil
}

// readMessages reads and processes incoming messages
func (c *BinanceFuturesWebSocketClient) readMessages() {
	defer c.wg.Done()
	defer func() {
		if c.conn != nil {
			c.conn.Close()
		}
	}()

	// Start heartbeat monitor
	c.wg.Add(1)
	go c.heartbeatMonitor()

	for c.isRunning.Load() {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Set a read deadline - pong handler will extend this
			c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))

			_, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Printf("WebSocket read error: %v", err)
				} else {
					log.Printf("WebSocket read error (non-unexpected): %v", err)
				}
				// Try to reconnect (only if still running)
				c.reconnect()
				return
			}

			c.processMessage(message)
			c.msgCounter.Add(1)
		}
	}
}

// processMessage handles incoming WebSocket messages
func (c *BinanceFuturesWebSocketClient) processMessage(data []byte) {
	// Skip ping/pong control frames are handled at control level; websocket.ReadMessage returns only application messages.
	// However some server control messages might be in JSON "result" etc — still handled below.

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
		if _, ok := controlMsg["error"]; ok {
			log.Printf("Received error from Binance: %v", controlMsg["error"])
			return
		}
	}

	// Log unhandled messages for debugging (skip empty messages)
	if len(data) > 10 {
		// Try to see what the message looks like
		log.Printf("DEBUG - Unhandled message structure: %s", string(data))
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

// reconnect attempts to reconnect with exponential backoff
func (c *BinanceFuturesWebSocketClient) reconnect() {
	if !c.isRunning.Load() {
		return
	}

	log.Println("Attempting to reconnect...")

	backoff := time.Second
	maxBackoff := 30 * time.Second
	attempts := 0

	for c.isRunning.Load() {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff):
			attempts++
			log.Printf("Reconnection attempt %d", attempts)

			// Close old connection if exists
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}

			// Try to reconnect
			if err := c.Start(); err != nil {
				log.Printf("Reconnection failed: %v", err)

				// Exponential backoff with jitter
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}

				// Add jitter
				jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
				backoff += jitter
				continue
			}

			// Reconnection successful
			log.Println("Reconnected successfully")
			return
		}
	}
}

// heartbeatMonitor sends periodic pings to keep connection alive
func (c *BinanceFuturesWebSocketClient) heartbeatMonitor() {
	defer c.wg.Done()

	// Binance recommends ping/pong every 3 minutes if idle; choose interval < read timeout
	interval := 3 * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for c.isRunning.Load() {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.conn != nil {
				// Send a WebSocket Ping control frame (NOT a JSON app message)
				deadline := time.Now().Add(5 * time.Second)
				payload := []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))
				if err := c.conn.WriteControl(websocket.PingMessage, payload, deadline); err != nil {
					log.Printf("Failed to send ping control frame: %v", err)
					// Consider triggering reconnect on write failure
				} else {
					log.Println("Sent ping control frame")
				}
			}
		}
	}
}

// Stop gracefully stops the client
func (c *BinanceFuturesWebSocketClient) Stop() {
	log.Println("Stopping Binance Futures WebSocket client...")

	c.isRunning.Store(false)
	c.cancel()

	// Close connection gracefully
	if c.conn != nil {
		_ = c.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.conn.Close()
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
