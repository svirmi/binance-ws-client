package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
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

// Configuration for WebSocket connections
type Config struct {
	// WebSocket settings
	MaxStreamsPerConnection int           `json:"max_streams_per_connection"`
	ReconnectWait           time.Duration `json:"reconnect_wait"`
	MaxReconnectWait        time.Duration `json:"max_reconnect_wait"`

	// Processing settings
	WorkerPoolSize    int           `json:"worker_pool_size"`
	ChannelBufferSize int           `json:"channel_buffer_size"`
	ReadTimeout       time.Duration `json:"read_timeout"`
}

// DefaultConfig returns a production-ready configuration
func DefaultConfig() *Config {
	return &Config{
		MaxStreamsPerConnection: 200,
		ReconnectWait:           2 * time.Second,
		MaxReconnectWait:        30 * time.Second,
		WorkerPoolSize:          runtime.NumCPU() * 2,
		ChannelBufferSize:       10000,
		ReadTimeout:             120 * time.Second,
	}
}

// Message wraps a WebSocket message with metadata
type Message struct {
	ClientID string          `json:"client_id"`
	Stream   string          `json:"stream"`
	Data     json.RawMessage `json:"data"`
	Received time.Time       `json:"received"`
}

// BookTickerEvent represents orderbook level1 (best bid/ask) from Binance
type BookTickerEvent struct {
	UpdateID  int64  `json:"u"`           // Order book updateId
	Symbol    string `json:"s"`           // Symbol
	BidPrice  string `json:"b"`           // Best bid price
	BidQty    string `json:"B"`           // Best bid quantity
	AskPrice  string `json:"a"`           // Best ask price
	AskQty    string `json:"A"`           // Best ask quantity
	Timestamp int64  `json:"E,omitempty"` // Event time (for combined streams)
	EventTime int64  `json:"T,omitempty"` // Transaction time (for combined streams)
}

// WebSocketClient manages a single WebSocket connection
type WebSocketClient struct {
	url           string
	conn          *websocket.Conn
	messageChan   chan Message
	handler       func(Message)
	reconnect     bool
	reconnectWait time.Duration
	maxWait       time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	wg            sync.WaitGroup
	isConnected   atomic.Bool
	subscriptions map[string]struct{}
	workerCount   int
	clientID      string
	config        *Config
	stats         *ClientStats
}

// ClientStats holds connection statistics
type ClientStats struct {
	messagesReceived atomic.Int64
	connectionErrors atomic.Int64
	reconnects       atomic.Int64
	lastConnected    atomic.Value // time.Time
	lastError        atomic.Value // string (error message)
	bookTickers      atomic.Int64
}

// NewWebSocketClient creates a new WebSocket client
func NewWebSocketClient(clientID, url string, config *Config, handler func(Message)) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	stats := &ClientStats{}
	stats.lastConnected.Store(time.Time{})
	stats.lastError.Store("") // Store empty string instead of nil

	return &WebSocketClient{
		clientID:      clientID,
		url:           url,
		config:        config,
		messageChan:   make(chan Message, config.ChannelBufferSize),
		handler:       handler,
		reconnect:     true,
		reconnectWait: config.ReconnectWait,
		maxWait:       config.MaxReconnectWait,
		ctx:           ctx,
		cancel:        cancel,
		isConnected:   atomic.Bool{},
		subscriptions: make(map[string]struct{}),
		workerCount:   config.WorkerPoolSize,
		stats:         stats,
	}
}

// Stats returns current client statistics
func (c *WebSocketClient) Stats() map[string]interface{} {
	lastErr := c.stats.lastError.Load()
	errMsg, _ := lastErr.(string)

	lastConn := c.stats.lastConnected.Load()
	lastConnTime, _ := lastConn.(time.Time)

	return map[string]interface{}{
		"client_id":         c.clientID,
		"connected":         c.isConnected.Load(),
		"messages_received": c.stats.messagesReceived.Load(),
		"connection_errors": c.stats.connectionErrors.Load(),
		"reconnects":        c.stats.reconnects.Load(),
		"book_tickers":      c.stats.bookTickers.Load(),
		"last_connected":    lastConnTime,
		"last_error":        errMsg,
		"subscriptions":     len(c.subscriptions),
	}
}

// Subscribe adds new streams to the subscription list
func (c *WebSocketClient) Subscribe(streams ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stream := range streams {
		// Remove validation - let Binance validate the streams
		c.subscriptions[strings.ToLower(stream)] = struct{}{}
	}

	log.Printf("[%s] Subscribed to %d streams: %v", c.clientID, len(streams), streams)

	if c.isConnected.Load() {
		if conn := c.getConnection(); conn != nil {
			return c.sendSubscription(conn)
		}
	}
	return nil
}

// getConnection safely returns the current connection
func (c *WebSocketClient) getConnection() *websocket.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// setConnection safely sets the current connection
func (c *WebSocketClient) setConnection(conn *websocket.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

// sendSubscription sends a subscription message to Binance
func (c *WebSocketClient) sendSubscription(conn *websocket.Conn) error {
	c.mu.RLock()
	streams := make([]string, 0, len(c.subscriptions))
	for stream := range c.subscriptions {
		streams = append(streams, stream)
	}
	c.mu.RUnlock()

	if len(streams) == 0 {
		return nil
	}

	subMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     rand.Intn(1000),
	}

	c.mu.Lock()
	err := conn.WriteJSON(subMsg)
	c.mu.Unlock()

	if err != nil {
		c.stats.connectionErrors.Add(1)
		c.stats.lastError.Store(err.Error())
		return fmt.Errorf("failed to send subscription: %w", err)
	}

	return nil
}

// Start connects to the WebSocket and starts processing messages
func (c *WebSocketClient) Start() error {
	c.wg.Add(1)
	go c.run()
	return nil
}

// connect establishes a WebSocket connection with reliable DNS resolution
func (c *WebSocketClient) connect() error {
	if c.isConnected.Load() {
		return nil
	}

	// Create a resolver that uses multiple DNS servers
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 2 * time.Second}
			servers := []string{"8.8.8.8:53", "1.1.1.1:53", "8.8.4.4:53"}
			for _, server := range servers {
				conn, err := d.DialContext(ctx, "udp", server)
				if err == nil {
					return conn, nil
				}
			}
			return nil, errors.New("all DNS servers failed")
		},
	}

	// Create custom dialer with resolver and timeouts
	dialer := &net.Dialer{
		Resolver:  resolver,
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	wsDialer := websocket.Dialer{
		NetDialContext:   dialer.DialContext,
		HandshakeTimeout: 30 * time.Second,
	}

	conn, resp, err := wsDialer.DialContext(c.ctx, c.url, nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("[%s] Connection failed: %v, response: %s",
				c.clientID, err, string(body))
			resp.Body.Close()
		}
		c.stats.connectionErrors.Add(1)
		c.stats.lastError.Store(err.Error())
		return err
	}

	c.setConnection(conn)
	c.isConnected.Store(true)
	c.stats.lastConnected.Store(time.Now())
	c.stats.lastError.Store("") // Clear error on successful connection
	log.Printf("[%s] Connected to %s", c.clientID, c.url)

	c.startWorkers()

	if len(c.subscriptions) > 0 {
		if err := c.sendSubscription(conn); err != nil {
			log.Printf("[%s] Failed to send subscription: %v", c.clientID, err)
		}
	}

	return nil
}

// disconnect closes the WebSocket connection
func (c *WebSocketClient) disconnect() {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	c.isConnected.Store(false)
	log.Printf("[%s] Disconnected", c.clientID)
}

// run manages the WebSocket connection lifecycle
func (c *WebSocketClient) run() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			c.disconnect()
			return
		default:
			if err := c.connect(); err != nil {
				log.Printf("[%s] Connection failed: %v", c.clientID, err)
				if c.reconnect {
					c.reconnectWithBackoff()
					continue
				}
				return
			}

			// Connection successful, start reading messages
			if err := c.readMessages(); err != nil {
				log.Printf("[%s] Read messages failed: %v", c.clientID, err)
				c.disconnect()
				if c.reconnect {
					c.reconnectWithBackoff()
					continue
				}
				return
			}
		}
	}
}

// reconnectWithBackoff attempts to reconnect with exponential backoff
func (c *WebSocketClient) reconnectWithBackoff() {
	wait := c.reconnectWait
	attempts := 0

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			c.stats.reconnects.Add(1)
			log.Printf("[%s] Reconnection attempt %d, waiting %v",
				c.clientID, attempts+1, wait)

			timer := time.NewTimer(wait)
			select {
			case <-c.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
				// Timer expired, try to reconnect
				return
			}

			// Increase wait time with jitter
			jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
			wait = wait*2 + jitter
			if wait > c.maxWait {
				wait = c.maxWait
			}
			attempts++
		}
	}
}

// readMessages reads incoming WebSocket messages
func (c *WebSocketClient) readMessages() error {
	conn := c.getConnection()
	if conn == nil {
		return errors.New("no connection")
	}

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
			_, message, err := conn.ReadMessage()
			if err != nil {
				if errors.Is(err, websocket.ErrCloseSent) ||
					websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return nil
				}
				return fmt.Errorf("read error: %w", err)
			}

			c.stats.messagesReceived.Add(1)
			msg := c.parseMessage(message)

			select {
			case c.messageChan <- msg:
				// Message sent to channel successfully
			case <-time.After(100 * time.Millisecond):
				log.Printf("[%s] Message channel blocked, dropping message", c.clientID)
			case <-c.ctx.Done():
				return nil
			}
		}
	}
}

// parseMessage parses incoming messages and extracts stream information
func (c *WebSocketClient) parseMessage(data []byte) Message {
	// Try raw event first - !bookTicker sends raw events without stream wrapper
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(data, &rawEvent); err == nil {
		// Check if this is a subscription result/error
		if _, hasResult := rawEvent["result"]; hasResult {
			return Message{
				ClientID: c.clientID,
				Stream:   "",
				Data:     data,
				Received: time.Now().UTC(),
			}
		}

		if _, hasError := rawEvent["error"]; hasError {
			return Message{
				ClientID: c.clientID,
				Stream:   "",
				Data:     data,
				Received: time.Now().UTC(),
			}
		}

		// Check if this is a bookTicker event (has symbol, bid price, and ask price)
		if symbol, ok := rawEvent["s"].(string); ok && symbol != "" {
			if _, hasBid := rawEvent["b"]; hasBid {
				if _, hasAsk := rawEvent["a"]; hasAsk {
					// This is a bookTicker event from !bookTicker combined stream
					log.Printf("[%s] Received book ticker for: %s", c.clientID, symbol)
					return Message{
						ClientID: c.clientID,
						Stream:   "!bookTicker",
						Data:     data,
						Received: time.Now().UTC(),
					}
				}
			}
		}
	}

	// If we can't parse it, still return a message for debugging
	log.Printf("[%s] Could not parse message: %s", c.clientID, string(data))
	return Message{
		ClientID: c.clientID,
		Stream:   "",
		Data:     data,
		Received: time.Now().UTC(),
	}
}

// startWorkers runs worker goroutines to process messages
func (c *WebSocketClient) startWorkers() {
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()

			for {
				select {
				case <-c.ctx.Done():
					return
				case msg := <-c.messageChan:
					// Handle Binance ping messages
					if bytes.Contains(msg.Data, []byte(`"ping"`)) {
						pong := bytes.Replace(msg.Data, []byte(`"ping"`), []byte(`"pong"`), 1)
						conn := c.getConnection()
						if conn != nil {
							c.mu.Lock()
							conn.WriteMessage(websocket.TextMessage, pong)
							c.mu.Unlock()
						}
						continue
					}

					c.handler(msg)
				}
			}
		}(i)
	}
}

// Shutdown gracefully stops the client
func (c *WebSocketClient) Shutdown() {
	log.Printf("[%s] Shutting down", c.clientID)
	c.cancel()
	c.disconnect()
	c.wg.Wait()
	close(c.messageChan)
	stats := c.Stats()
	log.Printf("[%s] Shutdown complete. Stats: messages=%v, bookTickers=%v",
		c.clientID, stats["messages_received"], stats["book_tickers"])
}

// StreamManager manages multiple WebSocket connections
type StreamManager struct {
	config  *Config
	clients []*WebSocketClient
	symbols []string
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stats   *StreamManagerStats
}

// StreamManagerStats holds statistics for the stream manager
type StreamManagerStats struct {
	totalMessages    atomic.Int64
	totalClients     atomic.Int64
	totalBookTickers atomic.Int64
	startTime        time.Time
}

// NewStreamManager creates a new stream manager
func NewStreamManager(config *Config, symbols []string) *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamManager{
		config:  config,
		symbols: symbols,
		ctx:     ctx,
		cancel:  cancel,
		stats: &StreamManagerStats{
			startTime: time.Now(),
		},
	}
}

// createMessageHandler creates a handler that logs messages to console
func (sm *StreamManager) createMessageHandler() func(Message) {
	return func(msg Message) {
		sm.stats.totalMessages.Add(1)

		if msg.Stream == "" {
			// Log control messages (subscription responses, errors, etc.)
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err == nil {
				if _, hasResult := data["result"]; hasResult {
					// Log subscription confirmation
					log.Printf("[%s] Subscription confirmed", msg.ClientID)
				} else if errMsg, ok := data["error"]; ok {
					log.Printf("[%s] Error: %v", msg.ClientID, errMsg)
				} else {
					// Debug: log unknown control messages
					log.Printf("[%s] Unknown control message: %s", msg.ClientID, string(msg.Data))
				}
			}
			return
		}

		// Parse and log book ticker messages
		if msg.Stream == "!bookTicker" {
			sm.handleBookTickerMessage(msg)
		} else {
			// Generic log for other stream types
			log.Printf("[%s] Unknown stream type: %s, Data: %s",
				msg.ClientID, msg.Stream, string(msg.Data))
		}
	}
}

// handleBookTickerMessage processes book ticker messages
func (sm *StreamManager) handleBookTickerMessage(msg Message) {
	var ticker BookTickerEvent

	// Parse the book ticker event
	if err := json.Unmarshal(msg.Data, &ticker); err != nil {
		log.Printf("[%s] BookTicker unmarshal error: %v, Data: %s",
			msg.ClientID, err, string(msg.Data))
		return
	}

	// Validate we have required fields
	if ticker.Symbol == "" || ticker.BidPrice == "" || ticker.AskPrice == "" {
		log.Printf("[%s] Invalid book ticker data: %+v", msg.ClientID, ticker)
		return
	}

	sm.stats.totalBookTickers.Add(1)

	// Update client stats if available
	if client := sm.findClient(msg.ClientID); client != nil {
		client.stats.bookTickers.Add(1)
	}

	// Use timestamp if available, otherwise use event time or received time
	var timestamp int64
	if ticker.Timestamp > 0 {
		timestamp = ticker.Timestamp
	} else if ticker.EventTime > 0 {
		timestamp = ticker.EventTime
	}

	var timeStr string
	if timestamp > 0 {
		timeStr = time.Unix(0, timestamp*int64(time.Millisecond)).UTC().Format("15:04:05.000")
	} else {
		timeStr = msg.Received.Format("15:04:05.000")
	}

	log.Printf("[%s] BOOK: %s Bid=%s/%s Ask=%s/%s Time=%s",
		msg.ClientID, ticker.Symbol, ticker.BidPrice, ticker.BidQty,
		ticker.AskPrice, ticker.AskQty, timeStr)
}

// findClient finds a client by ID
func (sm *StreamManager) findClient(clientID string) *WebSocketClient {
	for _, client := range sm.clients {
		if client.clientID == clientID {
			return client
		}
	}
	return nil
}

// Start initializes and starts all WebSocket connections
func (sm *StreamManager) Start() error {
	log.Printf("Starting stream manager for %d symbols", len(sm.symbols))

	handler := sm.createMessageHandler()

	// Create futures client for combined !bookTicker stream
	futuresBookTickerClient := NewWebSocketClient(
		"futures-bookticker",
		"wss://fstream.binance.com/stream",
		sm.config,
		handler,
	)

	// Subscribe to combined !bookTicker stream
	if err := futuresBookTickerClient.Subscribe("!bookTicker"); err != nil {
		log.Printf("Failed to subscribe futures bookTicker client: %v", err)
	} else {
		if err := futuresBookTickerClient.Start(); err != nil {
			log.Printf("Failed to start futures bookTicker client: %v", err)
		} else {
			sm.clients = append(sm.clients, futuresBookTickerClient)
			sm.stats.totalClients.Add(1)
		}
	}

	log.Printf("Started %d WebSocket client(s)", len(sm.clients))

	if len(sm.clients) == 0 {
		return errors.New("no WebSocket clients started")
	}

	// Start statistics reporter
	sm.wg.Add(1)
	go sm.reportStats()

	return nil
}

// reportStats periodically reports statistics
func (sm *StreamManager) reportStats() {
	defer sm.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			uptime := time.Since(sm.stats.startTime)
			log.Printf("STATS: Uptime=%v, TotalMsgs=%d, BookTickers=%d, ActiveClients=%d",
				uptime.Round(time.Second),
				sm.stats.totalMessages.Load(),
				sm.stats.totalBookTickers.Load(),
				sm.stats.totalClients.Load())

			// Report per-client stats
			for _, client := range sm.clients {
				stats := client.Stats()
				if connected, _ := stats["connected"].(bool); connected {
					log.Printf("  Client %s: msgs=%v, bookTickers=%v",
						stats["client_id"],
						stats["messages_received"],
						stats["book_tickers"])
				}
			}
		}
	}
}

// GetStats returns current statistics
func (sm *StreamManager) GetStats() map[string]interface{} {
	clientStats := make([]map[string]interface{}, len(sm.clients))
	for i, client := range sm.clients {
		clientStats[i] = client.Stats()
	}

	return map[string]interface{}{
		"total_clients":     sm.stats.totalClients.Load(),
		"total_messages":    sm.stats.totalMessages.Load(),
		"total_booktickers": sm.stats.totalBookTickers.Load(),
		"uptime":            time.Since(sm.stats.startTime).String(),
		"clients":           clientStats,
		"symbols_count":     len(sm.symbols),
	}
}

// Close gracefully shuts down all connections
func (sm *StreamManager) Close() {
	log.Println("Shutting down stream manager...")
	sm.cancel()

	for _, client := range sm.clients {
		client.Shutdown()
	}

	sm.wg.Wait()
	log.Println("Stream manager shut down")
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	config := DefaultConfig()

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
	}

	log.Printf("Starting crypto data collector for %d symbols", len(symbols))

	streamManager := NewStreamManager(config, symbols)
	if err := streamManager.Start(); err != nil {
		log.Fatalf("Failed to start stream manager: %v", err)
	}

	log.Printf("System started: %d client(s), %d workers",
		len(streamManager.clients), config.WorkerPoolSize)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Monitor memory usage
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Printf("MEMORY: Alloc=%.2fMB, TotalAlloc=%.2fMB, Sys=%.2fMB, NumGC=%d",
					float64(m.Alloc)/1024/1024,
					float64(m.TotalAlloc)/1024/1024,
					float64(m.Sys)/1024/1024,
					m.NumGC)
			case <-streamManager.ctx.Done():
				return
			}
		}
	}()

	// Wait for interrupt
	sig := <-sigChan
	log.Printf("Received %s, shutting down...", sig)

	startShutdown := time.Now()
	streamManager.Close()

	finalStats := streamManager.GetStats()
	log.Printf("FINAL STATS: %+v", finalStats)
	log.Printf("Shutdown completed in %v", time.Since(startShutdown))
}
