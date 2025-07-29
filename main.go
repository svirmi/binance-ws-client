package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketClient manages a WebSocket connection with ping/pong, reconnect, and graceful shutdown.
type WebSocketClient struct {
	url           string             // WebSocket URL (e.g., wss://stream.binance.com:9443/ws)
	conn          *websocket.Conn    // Current WebSocket connection
	messageChan   chan []byte        // Channel for received messages
	handler       func([]byte)       // Callback for processing messages
	reconnect     bool               // Whether to reconnect on failure
	reconnectWait time.Duration      // Initial wait time for reconnection
	maxWait       time.Duration      // Max wait time for exponential backoff
	ctx           context.Context    // Context for cancellation
	cancel        context.CancelFunc // Cancel function for shutdown
	mu            sync.Mutex         // Protects connection state
	wg            sync.WaitGroup     // Wait for goroutines to finish
	isConnected   bool               // Tracks connection status
}

// NewWebSocketClient creates a new WebSocket client.
func NewWebSocketClient(url string, handler func([]byte), reconnect bool) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebSocketClient{
		url:           url,
		messageChan:   make(chan []byte, 100), // Buffered channel for messages
		handler:       handler,
		reconnect:     reconnect,
		reconnectWait: time.Second,
		maxWait:       30 * time.Second,
		ctx:           ctx,
		cancel:        cancel,
		isConnected:   false,
	}
}

// Start connects to the WebSocket and starts reading messages.
func (c *WebSocketClient) Start() error {
	c.wg.Add(1)
	go c.run()
	return c.connect()
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

// connect establishes a WebSocket connection and starts reading messages.
func (c *WebSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected {
		return nil
	}

	conn, _, err := websocket.DefaultDialer.DialContext(c.ctx, c.url, nil)
	if err != nil {
		log.Printf("Connection failed: %v", err)
		return err
	}

	c.conn = conn
	c.isConnected = true
	log.Printf("Connected to %s", c.url)

	// Start reading messages
	c.wg.Add(1)
	go c.readMessages()

	// Start ping/pong handling
	c.wg.Add(1)
	go c.handlePingPong()

	return nil
}

// disconnect closes the WebSocket connection.
func (c *WebSocketClient) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.isConnected = false
		log.Printf("Disconnected from %s", c.url)
	}
}

// reconnectWithBackoff attempts to reconnect with exponential backoff.
func (c *WebSocketClient) reconnectWithBackoff() {
	wait := c.reconnectWait
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.connect(); err == nil {
				return
			}
			log.Printf("Reconnecting in %v...", wait)
			time.Sleep(wait)
			wait = wait * 2
			if wait > c.maxWait {
				wait = c.maxWait
			}
		}
	}
}

// readMessages reads incoming WebSocket messages and passes them to the handler.
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
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v", err)
				c.disconnect()
				return
			}
			// Pass message to handler via channel for concurrency safety
			select {
			case c.messageChan <- message:
			default:
				log.Println("Message channel full, dropping message")
			}
		}
	}
}

// handlePingPong responds to ping messages to keep the connection alive.
func (c *WebSocketClient) handlePingPong() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case message := <-c.messageChan:
			// Check for Binance ping message
			if string(message) == `{"event":"ping"}` {
				c.mu.Lock()
				if c.isConnected && c.conn != nil {
					if err := c.conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"pong"}`)); err != nil {
						log.Printf("Pong error: %v", err)
						c.disconnect()
					}
				}
				c.mu.Unlock()
			} else {
				// Pass non-ping messages to the handler
				if c.handler != nil {
					go c.handler(message)
				}
			}
		}
	}
}

// Shutdown gracefully stops the client.
func (c *WebSocketClient) Shutdown() {
	c.cancel()
	c.disconnect()
	c.wg.Wait()
	log.Println("WebSocket client shut down")
}

// Example usage
func main() {
	// Example handler for processing Binance trade messages
	handler := func(message []byte) {
		fmt.Printf("Received message: %s\n", string(message))
		// Add your trading signal logic here, e.g., parse JSON and process trade data
	}

	// Create client for Binance Spot trade stream
	client := NewWebSocketClient("wss://stream.binance.com:9443/ws/btcusdt@trade", handler, true)
	if err := client.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	// Run for a while, then gracefully shut down
	time.Sleep(10 * time.Second)
	client.Shutdown()
}
