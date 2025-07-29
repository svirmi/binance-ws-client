package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketClient manages a single WebSocket connection for Spot or Futures.
type WebSocketClient struct {
	url           string              // WebSocket base URL
	conn          *websocket.Conn     // Current WebSocket connection
	messageChan   chan Message        // Channel for received messages
	handler       func(Message)       // Handler for stream-specific messages
	reconnect     bool                // Whether to reconnect on failure
	reconnectWait time.Duration       // Initial wait time for reconnection
	maxWait       time.Duration       // Max wait time for exponential backoff
	ctx           context.Context     // Context for cancellation
	cancel        context.CancelFunc  // Cancel function for shutdown
	mu            sync.Mutex          // Protects connection state and subscriptions
	wg            sync.WaitGroup      // Wait for goroutines to finish
	isConnected   bool                // Tracks connection status
	subscriptions map[string]struct{} // Tracks subscribed streams
	workerCount   int                 // Number of worker goroutines
	clientID      string              // Identifier (e.g., "spot", "futures")
	isFutures     bool                // Whether this is a futures client
}

// Message wraps a WebSocket message with metadata.
type Message struct {
	ClientID string          // "spot" or "futures"
	Stream   string          // Stream name (e.g., "btcusdt@trade")
	Data     json.RawMessage // Raw message data
}

// WebSocketManager coordinates multiple WebSocket clients.
type WebSocketManager struct {
	clients map[string]*WebSocketClient // Map of clientID to client
	mu      sync.Mutex                  // Protects clients map
	wg      sync.WaitGroup              // Wait for all clients to shut down
}

// NewWebSocketManager creates a new manager.
func NewWebSocketManager() *WebSocketManager {
	return &WebSocketManager{
		clients: make(map[string]*WebSocketClient),
	}
}

// AddClient adds a WebSocket client to the manager.
func (m *WebSocketManager) AddClient(clientID, url string, handler func(Message), workerCount int) *WebSocketClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	client := NewWebSocketClient(clientID, url, handler, workerCount)
	m.clients[clientID] = client
	return client
}

// StartAll starts all managed clients.
func (m *WebSocketManager) StartAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, client := range m.clients {
		if err := client.Start(); err != nil {
			return fmt.Errorf("failed to start client %s: %v", client.clientID, err)
		}
	}
	return nil
}

// ShutdownAll gracefully shuts down all clients.
func (m *WebSocketManager) ShutdownAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, client := range m.clients {
		client.Shutdown()
	}
	m.wg.Wait()
	log.Println("All WebSocket clients shut down")
}

// NewWebSocketClient creates a new WebSocket client.
func NewWebSocketClient(clientID, url string, handler func(Message), workerCount int) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	isFutures := strings.Contains(url, "fapi")
	return &WebSocketClient{
		clientID:      clientID,
		url:           url,
		messageChan:   make(chan Message, 2000), // Increased buffer
		handler:       handler,
		reconnect:     true,
		reconnectWait: 2 * time.Second,
		maxWait:       30 * time.Second,
		ctx:           ctx,
		cancel:        cancel,
		isConnected:   false,
		subscriptions: make(map[string]struct{}),
		workerCount:   workerCount,
		isFutures:     isFutures,
	}
}

// Start connects to the WebSocket and starts processing messages.
func (c *WebSocketClient) Start() error {
	c.wg.Add(1)
	go c.run()
	return c.connect()
}

// Subscribe adds new streams to the subscription list with validation.
func (c *WebSocketClient) Subscribe(streams ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		if !strings.Contains(stream, "@") || len(strings.Split(stream, "@")) != 2 {
			return fmt.Errorf("invalid stream format: %s", stream)
		}
		c.subscriptions[strings.ToLower(stream)] = struct{}{}
	}
	log.Printf("[%s] Subscribed to streams: %v", c.clientID, streams)
	if c.isConnected && c.conn != nil {
		return c.sendSubscription()
	}
	return nil
}

// Unsubscribe removes streams from the subscription list.
func (c *WebSocketClient) Unsubscribe(streams ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		delete(c.subscriptions, strings.ToLower(stream))
	}
	log.Printf("[%s] Unsubscribed from streams: %v", c.clientID, streams)
	if c.isConnected && c.conn != nil {
		return c.sendSubscription()
	}
	return nil
}

// sendSubscription sends a subscription message to Binance.
func (c *WebSocketClient) sendSubscription() error {
	if len(c.subscriptions) == 0 {
		return nil
	}
	streams := make([]string, 0, len(c.subscriptions))
	for stream := range c.subscriptions {
		streams = append(streams, stream)
	}
	log.Printf("[%s] Sending subscription for streams: %v", c.clientID, streams)

	var subMsg map[string]interface{}

	if c.isFutures {
		// Futures API uses different format
		subMsg = map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": streams,
			"id":     rand.Intn(1000),
		}
	} else {
		// Spot API
		subMsg = map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": streams,
			"id":     rand.Intn(1000),
		}
	}

	// Log the exact subscription message
	msgBytes, _ := json.Marshal(subMsg)
	log.Printf("[%s] Subscription message: %s", c.clientID, string(msgBytes))
	return c.conn.WriteJSON(subMsg)
}

// run manages the WebSocket connection lifecycle.
func (c *WebSocketClient) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			c.disconnect()
			return
		default:
			if c.reconnect {
				c.reconnectWithBackoff()
			} else {
				return
			}
		}
	}
}

// lookupHostWithRetry attempts DNS resolution with retries and fallback resolvers.
func lookupHostWithRetry(host string, retries int, timeout time.Duration) ([]string, error) {
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}
	resolvers := []string{"8.8.8.8:53", "1.1.1.1:53"}
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			return d.DialContext(ctx, network, address)
		},
	}
	for attempt := 0; attempt < retries; attempt++ {
		for _, addr := range resolvers {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			addrs, err := resolver.LookupHost(ctx, host)
			cancel()
			if err == nil {
				return addrs, nil
			}
			log.Printf("DNS lookup attempt %d failed for %s on %s: %v", attempt+1, host, addr, err)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("DNS lookup failed after %d retries for %s", retries, host)
}

// connect establishes a WebSocket connection and subscribes to streams.
func (c *WebSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isConnected {
		return nil
	}
	host := strings.Split(strings.TrimPrefix(c.url, "wss://"), "/")[0]
	addrs, err := lookupHostWithRetry(host, 3, 2*time.Second)
	if err != nil {
		log.Printf("[%s] DNS lookup failed after retries: %v", c.clientID, err)
		return err
	}
	log.Printf("[%s] Resolved IPs for %s: %v", c.clientID, host, addrs)

	// Use combined stream URL for better performance
	var url string
	if c.isFutures {
		url = "wss://fstream.binance.com/ws"
	} else {
		url = "wss://stream.binance.com:9443/ws"
	}

	log.Printf("[%s] Connecting to URL: %s", c.clientID, url)
	dialer := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
	}
	conn, resp, err := dialer.DialContext(c.ctx, url, nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("[%s] Handshake failed with status: %v, response: %s", c.clientID, resp.Status, string(body))
			resp.Body.Close()
		} else {
			log.Printf("[%s] Connection failed: %v", c.clientID, err)
		}
		return err
	}
	c.conn = conn
	c.isConnected = true
	log.Printf("[%s] Connected to %s", c.clientID, url)

	c.wg.Add(1)
	go c.readMessages()
	c.wg.Add(1)
	go c.handlePingPong()
	c.startWorkers()

	if len(c.subscriptions) > 0 {
		return c.sendSubscription()
	}
	return nil
}

// disconnect closes the WebSocket connection.
func (c *WebSocketClient) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("[%s] Error closing connection: %v", c.clientID, err)
		}
		c.conn = nil
		c.isConnected = false
		log.Printf("[%s] Disconnected from %s", c.clientID, c.url)
	}
}

// reconnectWithBackoff attempts to reconnect with exponential backoff and jitter.
func (c *WebSocketClient) reconnectWithBackoff() {
	wait := c.reconnectWait
	attempts := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.connect(); err != nil {
				if strings.Contains(err.Error(), "close 1008") {
					log.Printf("[%s] Policy violation detected, waiting 5 minutes before retry", c.clientID)
					time.Sleep(5 * time.Minute)
				}
			} else {
				attempts = 0
				return
			}
			attempts++
			if attempts > 5 {
				log.Printf("[%s] Alert: Possible IP ban after %d attempts", c.clientID, attempts)
				time.Sleep(5 * time.Minute)
			}
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond
			log.Printf("[%s] Reconnecting (attempt %d) in %v...", c.clientID, attempts, wait+jitter)
			time.Sleep(wait + jitter)
			wait = wait * 2
			if wait > c.maxWait {
				wait = c.maxWait
			}
		}
	}
}

// readMessages reads incoming WebSocket messages.
func (c *WebSocketClient) readMessages() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if !c.isConnected {
				return
			}
			c.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.ClosePolicyViolation) {
					log.Printf("[%s] WebSocket closed: %v", c.clientID, err)
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Printf("[%s] Read timeout: %v", c.clientID, err)
				} else {
					log.Printf("[%s] Read error: %v", c.clientID, err)
				}
				c.disconnect()
				return
			}
			c.conn.SetReadDeadline(time.Time{})

			// Parse the message to extract stream info
			msg := c.parseMessage(message)

			select {
			case c.messageChan <- msg:
			default:
				log.Printf("[%s] Message channel full, dropping message", c.clientID)
			}
		}
	}
}

// parseMessage parses incoming messages and extracts stream information
func (c *WebSocketClient) parseMessage(data []byte) Message {
	// First, try to parse as a combined stream message
	var combinedMsg struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(data, &combinedMsg); err == nil && combinedMsg.Stream != "" {
		return Message{
			ClientID: c.clientID,
			Stream:   combinedMsg.Stream,
			Data:     combinedMsg.Data,
		}
	}

	// If not a combined stream message, try to parse the raw event
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(data, &rawEvent); err == nil {
		// Check if it's a subscription response or error
		if _, hasResult := rawEvent["result"]; hasResult {
			log.Printf("[%s] Subscription response: %s", c.clientID, string(data))
			return Message{ClientID: c.clientID, Stream: "", Data: data}
		}

		if _, hasError := rawEvent["error"]; hasError {
			log.Printf("[%s] Error response: %s", c.clientID, string(data))
			return Message{ClientID: c.clientID, Stream: "", Data: data}
		}

		// Try to infer stream from event data
		if eventType, ok := rawEvent["e"].(string); ok {
			if symbol, ok := rawEvent["s"].(string); ok {
				var stream string
				switch eventType {
				case "trade":
					stream = strings.ToLower(symbol) + "@trade"
				case "kline":
					stream = strings.ToLower(symbol) + "@kline_1m"
				}

				if stream != "" {
					return Message{
						ClientID: c.clientID,
						Stream:   stream,
						Data:     data,
					}
				}
			}
		}
	}

	// Default case - return message without stream
	return Message{
		ClientID: c.clientID,
		Stream:   "",
		Data:     data,
	}
}

// handlePingPong responds to ping messages and sends periodic pings.
func (c *WebSocketClient) handlePingPong() {
	defer c.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.isConnected && c.conn != nil {
				if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					log.Printf("[%s] Ping error: %v", c.clientID, err)
					c.disconnect()
				}
			}
			c.mu.Unlock()
		}
	}
}

// startWorkers runs a pool of workers to process messages.
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
					// Handle Binance ping
					if string(msg.Data) == `{"event":"ping"}` {
						c.mu.Lock()
						if c.isConnected && c.conn != nil {
							if err := c.conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"pong"}`)); err != nil {
								log.Printf("[%s] Pong error: %v", c.clientID, err)
								c.disconnect()
							}
						}
						c.mu.Unlock()
						continue
					}

					// Process the message with the handler
					c.handler(msg)
				}
			}
		}(i)
	}
}

// Shutdown gracefully stops the client.
func (c *WebSocketClient) Shutdown() {
	log.Printf("[%s] Initiating WebSocket client shutdown", c.clientID)
	c.cancel()
	c.disconnect()
	c.wg.Wait()
	close(c.messageChan)
	log.Printf("[%s] WebSocket client shut down", c.clientID)
}

// PriceData stores prices for trading signals.
type PriceData struct {
	mu     sync.RWMutex
	prices map[string]map[string]float64 // clientID -> symbol -> price
}

// TradeEvent represents a trade event from Binance
type TradeEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
	Ignore    bool   `json:"M"`
}

// KlineEvent represents a kline/candlestick event from Binance
type KlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     struct {
		StartTime           int64  `json:"t"`
		CloseTime           int64  `json:"T"`
		Symbol              string `json:"s"`
		Interval            string `json:"i"`
		FirstTradeID        int64  `json:"f"`
		LastTradeID         int64  `json:"L"`
		Open                string `json:"o"`
		Close               string `json:"c"`
		High                string `json:"h"`
		Low                 string `json:"l"`
		Volume              string `json:"v"`
		NumberOfTrades      int64  `json:"n"`
		IsClosed            bool   `json:"x"`
		QuoteVolume         string `json:"q"`
		TakerBuyBaseVolume  string `json:"V"`
		TakerBuyQuoteVolume string `json:"Q"`
		Ignore              string `json:"B"`
	} `json:"k"`
}

// Example usage with Spot and Futures
func main() {
	// Seed random for jitter
	rand.Seed(time.Now().UnixNano())

	// Thread-safe price storage
	prices := &PriceData{
		prices: make(map[string]map[string]float64),
	}

	// Thread-safe message counter
	var messageCounter struct {
		sync.Mutex
		count int
	}

	// Handler for processing Spot and Futures messages
	handler := func(msg Message) {
		// Increment message counter
		messageCounter.Lock()
		messageCounter.count++
		messageCounter.Unlock()

		// Skip if stream is empty (usually system messages)
		if msg.Stream == "" {
			// Don't log system messages as frequently
			return
		}

		// Log received message
		log.Printf("[%s] Processing stream %s", msg.ClientID, msg.Stream)

		// Parse message based on stream type
		if strings.HasSuffix(msg.Stream, "@trade") {
			var trade TradeEvent
			if err := json.Unmarshal(msg.Data, &trade); err != nil {
				log.Printf("[%s] Unmarshal error for trade stream %s: %v", msg.ClientID, msg.Stream, err)
				return
			}

			price, _ := strconv.ParseFloat(trade.Price, 64)
			prices.mu.Lock()
			if prices.prices[msg.ClientID] == nil {
				prices.prices[msg.ClientID] = make(map[string]float64)
			}
			prices.prices[msg.ClientID][trade.Symbol] = price
			prices.mu.Unlock()

			fmt.Printf("[%s] Trade - Stream: %s, Symbol: %s, Price: %.2f, Time: %d\n",
				msg.ClientID, msg.Stream, trade.Symbol, price, trade.TradeTime)

		} else if strings.Contains(msg.Stream, "@kline") {
			var kline KlineEvent
			if err := json.Unmarshal(msg.Data, &kline); err != nil {
				log.Printf("[%s] Unmarshal error for kline stream %s: %v", msg.ClientID, msg.Stream, err)
				return
			}

			price, _ := strconv.ParseFloat(kline.Kline.Close, 64)
			prices.mu.Lock()
			if prices.prices[msg.ClientID] == nil {
				prices.prices[msg.ClientID] = make(map[string]float64)
			}
			prices.prices[msg.ClientID][kline.Symbol] = price
			prices.mu.Unlock()

			fmt.Printf("[%s] Kline - Stream: %s, Symbol: %s, Close: %.2f\n",
				msg.ClientID, msg.Stream, kline.Symbol, price)
		} else {
			log.Printf("[%s] Unhandled stream: %s", msg.ClientID, msg.Stream)
		}
	}

	// Create manager
	manager := NewWebSocketManager()

	// Add Spot client
	spotClient := manager.AddClient("spot", "wss://stream.binance.com:9443/ws", handler, 4)
	spotStreams := []string{
		"btcusdt@trade",
		"ethusdt@trade",
	}
	if err := spotClient.Subscribe(spotStreams...); err != nil {
		log.Fatalf("Failed to subscribe to Spot streams: %v", err)
	}

	// Add Futures client
	futuresClient := manager.AddClient("futures", "wss://fstream.binance.com/ws", handler, 4)
	futuresStreams := []string{
		"btcusdt@trade",
		"ethusdt@kline_1m",
	}
	if err := futuresClient.Subscribe(futuresStreams...); err != nil {
		log.Fatalf("Failed to subscribe to Futures streams: %v", err)
	}

	// Start all clients
	if err := manager.StartAll(); err != nil {
		log.Fatalf("Failed to start clients: %v", err)
	}

	// Monitor message rate
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				messageCounter.Lock()
				if messageCounter.count > 0 {
					log.Printf("Messages processed in last 10 seconds: %d", messageCounter.count)
					messageCounter.count = 0
				}
				messageCounter.Unlock()
			}
		}
	}()

	// Handle SIGINT for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	log.Println("Shutting down...")
	manager.ShutdownAll()
}
