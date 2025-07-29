package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	return &WebSocketClient{
		clientID:      clientID,
		url:           url,
		messageChan:   make(chan Message, 1000), // Large buffer for multiple streams
		handler:       handler,
		reconnect:     true,
		reconnectWait: time.Second,
		maxWait:       30 * time.Second,
		ctx:           ctx,
		cancel:        cancel,
		isConnected:   false,
		subscriptions: make(map[string]struct{}),
		workerCount:   workerCount,
	}
}

// Start connects to the WebSocket and starts processing messages.
func (c *WebSocketClient) Start() error {
	c.wg.Add(1)
	go c.run()
	return c.connect()
}

// Subscribe adds new streams to the subscription list.
func (c *WebSocketClient) Subscribe(streams ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stream := range streams {
		c.subscriptions[strings.ToLower(stream)] = struct{}{}
	}

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

	subMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     rand.Intn(1000),
	}
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

// connect establishes a WebSocket connection and subscribes to streams.
func (c *WebSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected {
		return nil
	}

	// Build URL with combined streams
	streams := make([]string, 0, len(c.subscriptions))
	for stream := range c.subscriptions {
		streams = append(streams, stream)
	}
	url := c.url
	if len(streams) > 0 {
		url = fmt.Sprintf("%s/stream?streams=%s", c.url, strings.Join(streams, "/"))
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second, // Increased timeout
	}
	conn, _, err := dialer.DialContext(c.ctx, url, nil)
	if err != nil {
		log.Printf("[%s] Connection failed: %v", c.clientID, err)
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

	if len(streams) > 0 && !strings.Contains(c.url, "/stream") {
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
			if err := c.connect(); err == nil {
				attempts = 0
				return
			}
			attempts++
			if attempts > 5 {
				log.Printf("[%s] Alert: Possible IP ban after %d attempts", c.clientID, attempts)
				time.Sleep(5 * time.Minute) // Wait longer to avoid ban
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
			c.conn.SetReadDeadline(time.Now().Add(120 * time.Second)) // Increased timeout
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Printf("[%s] WebSocket closed normally: %v", c.clientID, err)
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Printf("[%s] Read timeout: %v", c.clientID, err)
				} else {
					log.Printf("[%s] Read error: %v", c.clientID, err)
				}
				c.disconnect()
				return
			}
			c.conn.SetReadDeadline(time.Time{})
			select {
			case c.messageChan <- Message{ClientID: c.clientID, Stream: "", Data: message}:
			default:
				log.Printf("[%s] Message channel full, dropping message", c.clientID)
			}
		}
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
			} else {
				// Parse combined stream payload
				var payload struct {
					Stream string          `json:"stream"`
					Data   json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(msg.Data, &payload); err == nil && payload.Stream != "" {
					msg.Stream = payload.Stream
					msg.Data = payload.Data
				}
				// Pass to message channel for worker processing
				select {
				case c.messageChan <- msg:
				default:
					log.Printf("[%s] Message channel full in handlePingPong, dropping message", c.clientID)
				}
			}
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
					// Process message in worker for parallel processing
					go c.handler(msg)
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

		type TradeEvent struct {
			EventType string `json:"e"`
			Symbol    string `json:"s"`
			Price     string `json:"p"`
			Quantity  string `json:"q"`
			TradeTime int64  `json:"T"`
		}
		type KlineEvent struct {
			Symbol string `json:"s"`
			Kline  struct {
				Open  string `json:"o"`
				Close string `json:"c"`
			} `json:"k"`
		}

		if strings.HasSuffix(msg.Stream, "@trade") {
			var trade TradeEvent
			if err := json.Unmarshal(msg.Data, &trade); err == nil {
				price, _ := strconv.ParseFloat(trade.Price, 64)
				prices.mu.Lock()
				if prices.prices[msg.ClientID] == nil {
					prices.prices[msg.ClientID] = make(map[string]float64)
				}
				prices.prices[msg.ClientID][trade.Symbol] = price
				prices.mu.Unlock()
				fmt.Printf("[%s] Trade - Stream: %s, Symbol: %s, Price: %.2f, Time: %d\n",
					msg.ClientID, msg.Stream, trade.Symbol, price, trade.TradeTime)
				// Add trading signal logic here
			} else {
				log.Printf("[%s] Unmarshal error for trade stream %s: %v", msg.ClientID, msg.Stream, err)
			}
		} else if strings.HasSuffix(msg.Stream, "@kline_1m") {
			var kline KlineEvent
			if err := json.Unmarshal(msg.Data, &kline); err == nil {
				price, _ := strconv.ParseFloat(kline.Kline.Close, 64)
				prices.mu.Lock()
				if prices.prices[msg.ClientID] == nil {
					prices.prices[msg.ClientID] = make(map[string]float64)
				}
				prices.prices[msg.ClientID][kline.Symbol] = price
				prices.mu.Unlock()
				fmt.Printf("[%s] Kline - Stream: %s, Symbol: %s, Close: %.2f\n",
					msg.ClientID, msg.Stream, kline.Symbol, price)
				// Add trading signal logic here
			} else {
				log.Printf("[%s] Unmarshal error for kline stream %s: %v", msg.ClientID, msg.Stream, err)
			}
		} else {
			fmt.Printf("[%s] Stream: %s, Data: %s\n", msg.ClientID, msg.Stream, string(msg.Data))
		}
	}

	// Create manager
	manager := NewWebSocketManager()

	// Add Spot client
	spotClient := manager.AddClient("spot", "wss://stream.binance.com:9443", handler, 4)
	spotStreams := []string{
		"btcusdt@trade",
		"ethusdt@trade",
		"bnbusdt@trade",
		"adausdt@trade",
		"xrpusdt@trade",
		// Add more Spot coins (e.g., 50 total)
	}
	if err := spotClient.Subscribe(spotStreams...); err != nil {
		log.Fatalf("Failed to subscribe to Spot streams: %v", err)
	}

	// Add Futures client
	futuresClient := manager.AddClient("futures", "wss://ws-fapi.binance.com/ws-fapi/v1", handler, 4)
	futuresStreams := []string{
		"btcusdt@trade",
		"ethusdt@kline_1m",
		"bnbusdt@trade",
		"adausdt@kline_1m",
		"xrpusdt@trade",
		// Add more Futures coins (e.g., 50 total)
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
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				messageCounter.Lock()
				if messageCounter.count > 0 {
					log.Printf("Messages per second: %d", messageCounter.count)
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

	manager.ShutdownAll()
}
