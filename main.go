package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Configuration for scalable processing
type Config struct {
	// WebSocket settings
	MaxStreamsPerConnection int           `json:"max_streams_per_connection"`
	ReconnectWait           time.Duration `json:"reconnect_wait"`
	MaxReconnectWait        time.Duration `json:"max_reconnect_wait"`

	// Processing settings
	WorkerPoolSize    int           `json:"worker_pool_size"`
	BatchSize         int           `json:"batch_size"`
	BatchTimeout      time.Duration `json:"batch_timeout"`
	ChannelBufferSize int           `json:"channel_buffer_size"`

	// Database settings
	DatabaseURL      string        `json:"database_url"`
	MaxDBConnections int           `json:"max_db_connections"`
	DBTimeout        time.Duration `json:"db_timeout"`
}

// DefaultConfig returns a production-ready configuration
func DefaultConfig() *Config {
	return &Config{
		MaxStreamsPerConnection: 200,
		ReconnectWait:           2 * time.Second,
		MaxReconnectWait:        30 * time.Second,
		WorkerPoolSize:          4,
		BatchSize:               50,
		BatchTimeout:            2 * time.Second,
		ChannelBufferSize:       5000,
		DatabaseURL:             "postgres://user:password@localhost/crypto_db?sslmode=disable",
		MaxDBConnections:        10,
		DBTimeout:               5 * time.Second,
	}
}

// Message wraps a WebSocket message with metadata
type Message struct {
	ClientID string          `json:"client_id"`
	Stream   string          `json:"stream"`
	Data     json.RawMessage `json:"data"`
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

// TradeData represents processed trade information
type TradeData struct {
	ClientID     string    `json:"client_id"`
	Symbol       string    `json:"symbol"`
	Price        float64   `json:"price"`
	Quantity     float64   `json:"quantity"`
	TradeTime    time.Time `json:"trade_time"`
	IsBuyerMaker bool      `json:"is_buyer_maker"`
	ProcessedAt  time.Time `json:"processed_at"`
}

// KlineData represents processed kline information
type KlineData struct {
	ClientID    string    `json:"client_id"`
	Symbol      string    `json:"symbol"`
	Interval    string    `json:"interval"`
	OpenTime    time.Time `json:"open_time"`
	CloseTime   time.Time `json:"close_time"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Close       float64   `json:"close"`
	Volume      float64   `json:"volume"`
	IsClosed    bool      `json:"is_closed"`
	ProcessedAt time.Time `json:"processed_at"`
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
	mu            sync.Mutex
	wg            sync.WaitGroup
	isConnected   bool
	subscriptions map[string]struct{}
	workerCount   int
	clientID      string
	isFutures     bool
}

// NewWebSocketClient creates a new WebSocket client
func NewWebSocketClient(clientID, url string, handler func(Message), workerCount int) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	isFutures := strings.Contains(url, "fstream")

	return &WebSocketClient{
		clientID:      clientID,
		url:           url,
		messageChan:   make(chan Message, 2000),
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

// Subscribe adds new streams to the subscription list
func (c *WebSocketClient) Subscribe(streams ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stream := range streams {
		if !strings.Contains(stream, "@") || len(strings.Split(stream, "@")) != 2 {
			return fmt.Errorf("invalid stream format: %s", stream)
		}
		c.subscriptions[strings.ToLower(stream)] = struct{}{}
	}

	log.Printf("[%s] Subscribed to %d streams", c.clientID, len(streams))

	if c.isConnected && c.conn != nil {
		return c.sendSubscription()
	}
	return nil
}

// sendSubscription sends a subscription message to Binance
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

// Start connects to the WebSocket and starts processing messages
func (c *WebSocketClient) Start() error {
	c.wg.Add(1)
	go c.run()
	return c.connect()
}

// lookupHostWithRetry attempts DNS resolution with retries
func lookupHostWithRetry(host string, retries int, timeout time.Duration) ([]string, error) {
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			return d.DialContext(ctx, network, "8.8.8.8:53")
		},
	}

	for attempt := 0; attempt < retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		addrs, err := resolver.LookupHost(ctx, host)
		cancel()

		if err == nil {
			return addrs, nil
		}

		if attempt < retries-1 {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	}

	return nil, fmt.Errorf("DNS lookup failed after %d retries for %s", retries, host)
}

// connect establishes a WebSocket connection
func (c *WebSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected {
		return nil
	}

	host := strings.Split(strings.TrimPrefix(c.url, "wss://"), "/")[0]
	_, err := lookupHostWithRetry(host, 3, 2*time.Second)
	if err != nil {
		log.Printf("[%s] DNS lookup failed: %v", c.clientID, err)
		return err
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
	}

	conn, resp, err := dialer.DialContext(c.ctx, c.url, nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("[%s] Connection failed with status: %v, response: %s", c.clientID, resp.Status, string(body))
			resp.Body.Close()
		}
		return err
	}

	c.conn = conn
	c.isConnected = true
	log.Printf("[%s] Connected to %s", c.clientID, c.url)

	c.wg.Add(2)
	go c.readMessages()
	go c.handlePingPong()
	c.startWorkers()

	if len(c.subscriptions) > 0 {
		return c.sendSubscription()
	}

	return nil
}

// disconnect closes the WebSocket connection
func (c *WebSocketClient) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.isConnected = false
		log.Printf("[%s] Disconnected", c.clientID)
	}
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
			if c.reconnect {
				c.reconnectWithBackoff()
			} else {
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
			if err := c.connect(); err != nil {
				attempts++
				if attempts > 5 {
					log.Printf("[%s] Multiple reconnection failures, waiting longer", c.clientID)
					time.Sleep(5 * time.Minute)
				}

				jitter := time.Duration(rand.Intn(100)) * time.Millisecond
				log.Printf("[%s] Reconnecting in %v (attempt %d)", c.clientID, wait+jitter, attempts)
				time.Sleep(wait + jitter)

				wait = wait * 2
				if wait > c.maxWait {
					wait = c.maxWait
				}
			} else {
				return
			}
		}
	}
}

// readMessages reads incoming WebSocket messages
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
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Printf("[%s] Read error: %v", c.clientID, err)
				}
				c.disconnect()
				return
			}
			c.conn.SetReadDeadline(time.Time{})

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
	// Try combined stream message first
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

	// Try raw event
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(data, &rawEvent); err == nil {
		if _, hasResult := rawEvent["result"]; hasResult {
			return Message{ClientID: c.clientID, Stream: "", Data: data}
		}

		if _, hasError := rawEvent["error"]; hasError {
			return Message{ClientID: c.clientID, Stream: "", Data: data}
		}

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

	return Message{
		ClientID: c.clientID,
		Stream:   "",
		Data:     data,
	}
}

// handlePingPong manages ping/pong messages
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
					if string(msg.Data) == `{"event":"ping"}` {
						c.mu.Lock()
						if c.isConnected && c.conn != nil {
							c.conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"pong"}`))
						}
						c.mu.Unlock()
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
}

// DatabaseManager handles all database operations
type DatabaseManager struct {
	db     *sql.DB
	config *Config

	tradeChan chan *TradeData
	klineChan chan *KlineData

	tradeBatch []*TradeData
	klineBatch []*KlineData

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager(config *Config) (*DatabaseManager, error) {
	db, err := sql.Open("postgres", config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	db.SetMaxOpenConns(config.MaxDBConnections)
	db.SetMaxIdleConns(config.MaxDBConnections / 2)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	dm := &DatabaseManager{
		db:         db,
		config:     config,
		tradeChan:  make(chan *TradeData, config.ChannelBufferSize),
		klineChan:  make(chan *KlineData, config.ChannelBufferSize),
		tradeBatch: make([]*TradeData, 0, config.BatchSize),
		klineBatch: make([]*KlineData, 0, config.BatchSize),
		ctx:        ctx,
		cancel:     cancel,
	}

	if err := dm.initTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize tables: %v", err)
	}

	return dm, nil
}

// initTables creates necessary database tables
func (dm *DatabaseManager) initTables() error {
	tradeTable := `
	CREATE TABLE IF NOT EXISTS trades (
		id SERIAL PRIMARY KEY,
		client_id VARCHAR(20) NOT NULL,
		symbol VARCHAR(20) NOT NULL,
		price DECIMAL(20,8) NOT NULL,
		quantity DECIMAL(20,8) NOT NULL,
		trade_time TIMESTAMP NOT NULL,
		is_buyer_maker BOOLEAN NOT NULL,
		processed_at TIMESTAMP DEFAULT NOW(),
		created_at TIMESTAMP DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades(symbol, trade_time);
	CREATE INDEX IF NOT EXISTS idx_trades_client_symbol ON trades(client_id, symbol);
	`

	klineTable := `
	CREATE TABLE IF NOT EXISTS klines (
		id SERIAL PRIMARY KEY,
		client_id VARCHAR(20) NOT NULL,
		symbol VARCHAR(20) NOT NULL,
		interval VARCHAR(10) NOT NULL,
		open_time TIMESTAMP NOT NULL,
		close_time TIMESTAMP NOT NULL,
		open_price DECIMAL(20,8) NOT NULL,
		high_price DECIMAL(20,8) NOT NULL,
		low_price DECIMAL(20,8) NOT NULL,
		close_price DECIMAL(20,8) NOT NULL,
		volume DECIMAL(20,8) NOT NULL,
		is_closed BOOLEAN NOT NULL,
		processed_at TIMESTAMP DEFAULT NOW(),
		created_at TIMESTAMP DEFAULT NOW(),
		UNIQUE(client_id, symbol, interval, open_time)
	);
	CREATE INDEX IF NOT EXISTS idx_klines_symbol_time ON klines(symbol, open_time);
	CREATE INDEX IF NOT EXISTS idx_klines_client_symbol ON klines(client_id, symbol, interval);
	`

	if _, err := dm.db.Exec(tradeTable); err != nil {
		return fmt.Errorf("failed to create trades table: %v", err)
	}

	if _, err := dm.db.Exec(klineTable); err != nil {
		return fmt.Errorf("failed to create klines table: %v", err)
	}

	return nil
}

// Start begins the database processing workers
func (dm *DatabaseManager) Start() {
	log.Println("Starting database manager...")

	dm.wg.Add(3)
	go dm.processTradeBatches()
	go dm.processKlineBatches()
	go dm.periodicFlush()
}

// AddTrade adds a trade to the processing queue
func (dm *DatabaseManager) AddTrade(trade *TradeData) {
	select {
	case dm.tradeChan <- trade:
	default:
		log.Printf("Trade channel full, dropping trade for %s", trade.Symbol)
	}
}

// AddKline adds a kline to the processing queue
func (dm *DatabaseManager) AddKline(kline *KlineData) {
	select {
	case dm.klineChan <- kline:
	default:
		log.Printf("Kline channel full, dropping kline for %s", kline.Symbol)
	}
}

// processTradeBatches processes trades in batches
func (dm *DatabaseManager) processTradeBatches() {
	defer dm.wg.Done()
	ticker := time.NewTicker(dm.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			dm.flushTrades()
			return

		case trade := <-dm.tradeChan:
			dm.mu.Lock()
			dm.tradeBatch = append(dm.tradeBatch, trade)
			shouldFlush := len(dm.tradeBatch) >= dm.config.BatchSize
			dm.mu.Unlock()

			if shouldFlush {
				dm.flushTrades()
			}

		case <-ticker.C:
			dm.flushTrades()
		}
	}
}

// processKlineBatches processes klines in batches
func (dm *DatabaseManager) processKlineBatches() {
	defer dm.wg.Done()
	ticker := time.NewTicker(dm.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			dm.flushKlines()
			return

		case kline := <-dm.klineChan:
			dm.mu.Lock()
			dm.klineBatch = append(dm.klineBatch, kline)
			shouldFlush := len(dm.klineBatch) >= dm.config.BatchSize
			dm.mu.Unlock()

			if shouldFlush {
				dm.flushKlines()
			}

		case <-ticker.C:
			dm.flushKlines()
		}
	}
}

// flushTrades writes accumulated trades to database
func (dm *DatabaseManager) flushTrades() {
	dm.mu.Lock()
	batch := dm.tradeBatch
	dm.tradeBatch = make([]*TradeData, 0, dm.config.BatchSize)
	dm.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(dm.ctx, dm.config.DBTimeout)
	defer cancel()

	tx, err := dm.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for trades: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO trades (client_id, symbol, price, quantity, trade_time, is_buyer_maker, processed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		log.Printf("Failed to prepare trade statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, trade := range batch {
		_, err := stmt.ExecContext(ctx,
			trade.ClientID, trade.Symbol, trade.Price, trade.Quantity,
			trade.TradeTime, trade.IsBuyerMaker, trade.ProcessedAt,
		)
		if err != nil {
			log.Printf("Failed to insert trade: %v", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit trades: %v", err)
		return
	}

	log.Printf("Inserted %d trades", len(batch))
}

// flushKlines writes accumulated klines to database
func (dm *DatabaseManager) flushKlines() {
	dm.mu.Lock()
	batch := dm.klineBatch
	dm.klineBatch = make([]*KlineData, 0, dm.config.BatchSize)
	dm.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(dm.ctx, dm.config.DBTimeout)
	defer cancel()

	tx, err := dm.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for klines: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO klines (client_id, symbol, interval, open_time, close_time, 
			open_price, high_price, low_price, close_price, volume, is_closed, processed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (client_id, symbol, interval, open_time) 
		DO UPDATE SET
			close_time = EXCLUDED.close_time,
			open_price = EXCLUDED.open_price,
			high_price = EXCLUDED.high_price,
			low_price = EXCLUDED.low_price,
			close_price = EXCLUDED.close_price,
			volume = EXCLUDED.volume,
			is_closed = EXCLUDED.is_closed,
			processed_at = EXCLUDED.processed_at
	`)
	if err != nil {
		log.Printf("Failed to prepare kline statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, kline := range batch {
		_, err := stmt.ExecContext(ctx,
			kline.ClientID, kline.Symbol, kline.Interval,
			kline.OpenTime, kline.CloseTime,
			kline.Open, kline.High, kline.Low, kline.Close,
			kline.Volume, kline.IsClosed, kline.ProcessedAt,
		)
		if err != nil {
			log.Printf("Failed to insert kline: %v", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit klines: %v", err)
		return
	}

	log.Printf("Inserted/updated %d klines", len(batch))
}

// periodicFlush ensures data is written even with low volume
func (dm *DatabaseManager) periodicFlush() {
	defer dm.wg.Done()
	ticker := time.NewTicker(dm.config.BatchTimeout * 2)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.flushTrades()
			dm.flushKlines()
		}
	}
}

// Close gracefully shuts down the database manager
func (dm *DatabaseManager) Close() {
	log.Println("Shutting down database manager...")
	dm.cancel()
	dm.wg.Wait()

	dm.flushTrades()
	dm.flushKlines()

	if err := dm.db.Close(); err != nil {
		log.Printf("Error closing database: %v", err)
	}
}

// StreamManager manages multiple WebSocket connections
type StreamManager struct {
	config    *Config
	dbManager *DatabaseManager
	clients   []*WebSocketClient
	symbols   []string
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewStreamManager creates a new stream manager
func NewStreamManager(config *Config, dbManager *DatabaseManager, symbols []string) *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamManager{
		config:    config,
		dbManager: dbManager,
		symbols:   symbols,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start initializes and starts all WebSocket connections
func (sm *StreamManager) Start() error {
	log.Printf("Starting stream manager for %d symbols", len(sm.symbols))

	connectionsNeeded := (len(sm.symbols)*2 + sm.config.MaxStreamsPerConnection - 1) / sm.config.MaxStreamsPerConnection
	if connectionsNeeded == 0 {
		connectionsNeeded = 1
	}

	log.Printf("Creating %d WebSocket connections", connectionsNeeded)

	handler := sm.createMessageHandler()

	spotStreams := make([][]string, connectionsNeeded)
	futuresStreams := make([][]string, connectionsNeeded)

	streamIndex := 0
	for _, symbol := range sm.symbols {
		connIndex := streamIndex % connectionsNeeded

		spotStreams[connIndex] = append(spotStreams[connIndex], strings.ToLower(symbol)+"@trade")
		futuresStreams[connIndex] = append(futuresStreams[connIndex], strings.ToLower(symbol)+"@trade")

		streamIndex++
	}

	for i := 0; i < connectionsNeeded; i++ {
		if len(spotStreams[i]) > 0 {
			spotClient := NewWebSocketClient(
				fmt.Sprintf("spot-%d", i),
				"wss://stream.binance.com:9443/ws",
				handler,
				sm.config.WorkerPoolSize,
			)

			if err := spotClient.Subscribe(spotStreams[i]...); err != nil {
				return fmt.Errorf("failed to subscribe spot client %d: %v", i, err)
			}

			sm.clients = append(sm.clients, spotClient)
		}

		if len(futuresStreams[i]) > 0 {
			futuresClient := NewWebSocketClient(
				fmt.Sprintf("futures-%d", i),
				"wss://fstream.binance.com/ws",
				handler,
				sm.config.WorkerPoolSize,
			)

			if err := futuresClient.Subscribe(futuresStreams[i]...); err != nil {
				return fmt.Errorf("failed to subscribe futures client %d: %v", i, err)
			}

			sm.clients = append(sm.clients, futuresClient)
		}
	}

	// Start all clients
	for _, client := range sm.clients {
		if err := client.Start(); err != nil {
			return fmt.Errorf("failed to start client %s: %v", client.clientID, err)
		}
	}

	log.Printf("Started %d WebSocket clients", len(sm.clients))
	return nil
}

// createMessageHandler creates a handler that processes messages and sends to database
func (sm *StreamManager) createMessageHandler() func(Message) {
	return func(msg Message) {
		if msg.Stream == "" {
			return
		}

		now := time.Now()

		if strings.HasSuffix(msg.Stream, "@trade") {
			var trade TradeEvent
			if err := json.Unmarshal(msg.Data, &trade); err != nil {
				log.Printf("[%s] Trade unmarshal error: %v", msg.ClientID, err)
				return
			}

			price, _ := strconv.ParseFloat(trade.Price, 64)
			quantity, _ := strconv.ParseFloat(trade.Quantity, 64)

			tradeData := &TradeData{
				ClientID:     msg.ClientID,
				Symbol:       trade.Symbol,
				Price:        price,
				Quantity:     quantity,
				TradeTime:    time.Unix(0, trade.TradeTime*int64(time.Millisecond)),
				IsBuyerMaker: trade.IsMaker,
				ProcessedAt:  now,
			}

			sm.dbManager.AddTrade(tradeData)

		} else if strings.Contains(msg.Stream, "@kline") {
			var kline KlineEvent
			if err := json.Unmarshal(msg.Data, &kline); err != nil {
				log.Printf("[%s] Kline unmarshal error: %v", msg.ClientID, err)
				return
			}

			open, _ := strconv.ParseFloat(kline.Kline.Open, 64)
			high, _ := strconv.ParseFloat(kline.Kline.High, 64)
			low, _ := strconv.ParseFloat(kline.Kline.Low, 64)
			close, _ := strconv.ParseFloat(kline.Kline.Close, 64)
			volume, _ := strconv.ParseFloat(kline.Kline.Volume, 64)

			klineData := &KlineData{
				ClientID:    msg.ClientID,
				Symbol:      kline.Symbol,
				Interval:    kline.Kline.Interval,
				OpenTime:    time.Unix(0, kline.Kline.StartTime*int64(time.Millisecond)),
				CloseTime:   time.Unix(0, kline.Kline.CloseTime*int64(time.Millisecond)),
				Open:        open,
				High:        high,
				Low:         low,
				Close:       close,
				Volume:      volume,
				IsClosed:    kline.Kline.IsClosed,
				ProcessedAt: now,
			}

			sm.dbManager.AddKline(klineData)
		}
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

// Statistics holds runtime statistics
type Statistics struct {
	mu               sync.RWMutex
	tradesProcessed  int64
	klinesProcessed  int64
	messagesReceived int64
	startTime        time.Time
}

// NewStatistics creates a new statistics tracker
func NewStatistics() *Statistics {
	return &Statistics{
		startTime: time.Now(),
	}
}

// IncrementTrades increments the trade counter
func (s *Statistics) IncrementTrades() {
	s.mu.Lock()
	s.tradesProcessed++
	s.mu.Unlock()
}

// IncrementKlines increments the kline counter
func (s *Statistics) IncrementKlines() {
	s.mu.Lock()
	s.klinesProcessed++
	s.mu.Unlock()
}

// IncrementMessages increments the message counter
func (s *Statistics) IncrementMessages() {
	s.mu.Lock()
	s.messagesReceived++
	s.mu.Unlock()
}

// GetStats returns current statistics
func (s *Statistics) GetStats() (int64, int64, int64, time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tradesProcessed, s.klinesProcessed, s.messagesReceived, time.Since(s.startTime)
}

// Example usage
func main() {
	// Seed random for jitter
	rand.Seed(time.Now().UnixNano())

	// Load configuration
	config := DefaultConfig()

	// Override database URL from environment if available
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		config.DatabaseURL = dbURL
	}

	// Define symbols to track
	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
		"SHIBUSDT", "MATICUSDT", "NEARUSDT", "ATOMUSDT", "LINKUSDT",
		"LTCUSDT", "BCHUSDT", "FILUSDT", "TRXUSDT", "ETCUSDT",
	}

	log.Printf("Starting crypto data collector for %d symbols", len(symbols))

	// Initialize statistics
	stats := NewStatistics()

	// Initialize database manager
	dbManager, err := NewDatabaseManager(config)
	if err != nil {
		log.Fatalf("Failed to initialize database manager: %v", err)
	}

	// Start database processing
	dbManager.Start()

	// Initialize stream manager
	streamManager := NewStreamManager(config, dbManager, symbols)

	// Start stream processing
	if err := streamManager.Start(); err != nil {
		log.Fatalf("Failed to start stream manager: %v", err)
	}

	// Statistics and health monitoring
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				trades, klines, messages, uptime := stats.GetStats()
				log.Printf("Runtime Stats - Uptime: %v, Messages: %d, Trades: %d, Klines: %d, Clients: %d",
					uptime.Round(time.Second), messages, trades, klines, len(streamManager.clients))
			}
		}
	}()

	// Memory usage monitoring
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Printf("Memory Stats - Alloc: %d KB, Sys: %d KB, NumGC: %d",
					m.Alloc/1024, m.Sys/1024, m.NumGC)
			}
		}
	}()

	log.Printf("System started successfully")
	log.Printf("- Tracking %d symbols", len(symbols))
	log.Printf("- Using %d WebSocket connections", len(streamManager.clients))
	log.Printf("- Database batch size: %d", config.BatchSize)
	log.Printf("- Worker pool size: %d", config.WorkerPoolSize)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	log.Println("Shutting down gracefully...")

	// Shutdown in reverse order
	streamManager.Close()
	dbManager.Close()

	trades, klines, messages, uptime := stats.GetStats()
	log.Printf("Final Stats - Uptime: %v, Messages: %d, Trades: %d, Klines: %d",
		uptime.Round(time.Second), messages, trades, klines)

	log.Println("Shutdown complete")
}
