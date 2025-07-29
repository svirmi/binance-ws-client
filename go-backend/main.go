package main

import (
	"bytes"
	"context"
	"database/sql"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq" // PostgreSQL driver for QuestDB
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

	// QuestDB settings
	QuestDBHost      string        `json:"questdb_host"`
	QuestDBPort      int           `json:"questdb_port"`
	QuestDBUser      string        `json:"questdb_user"`
	QuestDBPass      string        `json:"questdb_pass"`
	QuestDBName      string        `json:"questdb_name"`
	MaxDBConnections int           `json:"max_db_connections"`
	DBTimeout        time.Duration `json:"db_timeout"`
}

// DefaultConfig returns a production-ready configuration
func DefaultConfig() *Config {
	return &Config{
		MaxStreamsPerConnection: 200,
		ReconnectWait:           2 * time.Second,
		MaxReconnectWait:        30 * time.Second,
		WorkerPoolSize:          runtime.NumCPU() * 2,
		BatchSize:               1000, // Larger batches for QuestDB
		BatchTimeout:            1 * time.Second,
		ChannelBufferSize:       10000,
		QuestDBHost:             "localhost",
		QuestDBPort:             8812, // QuestDB PostgreSQL port
		QuestDBUser:             "admin",
		QuestDBPass:             "quest",
		QuestDBName:             "main",
		MaxDBConnections:        20,
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

// TradeData represents processed trade information for QuestDB
type TradeData struct {
	ClientID     string
	Symbol       string
	Price        float64
	Quantity     float64
	TradeTime    time.Time
	IsBuyerMaker bool
	ProcessedAt  time.Time
}

// KlineData represents processed kline information for QuestDB
type KlineData struct {
	ClientID    string
	Symbol      string
	Interval    string
	OpenTime    time.Time
	CloseTime   time.Time
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	IsClosed    bool
	ProcessedAt time.Time
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
		messageChan:   make(chan Message, 5000),
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

// DNS cache for improved resilience
var dnsCache = make(map[string][]string)
var dnsMutex sync.RWMutex

// lookupHostWithRetry attempts DNS resolution with retries
func lookupHostWithRetry(host string, retries int, timeout time.Duration) ([]string, error) {
	dnsMutex.RLock()
	cached, exists := dnsCache[host]
	dnsMutex.RUnlock()

	if exists {
		return cached, nil
	}

	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			return d.DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}

	for attempt := 0; attempt < retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		addrs, err := resolver.LookupHost(ctx, host)
		cancel()

		if err == nil {
			dnsMutex.Lock()
			dnsCache[host] = addrs
			dnsMutex.Unlock()
			return addrs, nil
		}

		if attempt < retries-1 {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	}

	return nil, fmt.Errorf("DNS lookup failed after %d retries for %s", retries, host)
}

// connect establishes a WebSocket connection with reliable DNS resolution
func (c *WebSocketClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected {
		return nil
	}

	// Create a resolver that uses multiple DNS servers
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 2 * time.Second}
			// Try multiple public DNS servers
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
		return err
	}

	c.conn = conn
	c.isConnected = true
	log.Printf("[%s] Connected to %s", c.clientID, c.url)

	c.wg.Add(1)
	go c.readMessages()
	c.startWorkers()

	if len(c.subscriptions) > 0 {
		if err := c.sendSubscription(); err != nil {
			log.Printf("[%s] Failed to send subscription: %v", c.clientID, err)
		}
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
				if errors.Is(err, websocket.ErrCloseSent) ||
					websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return
				}
				log.Printf("[%s] Read error: %v", c.clientID, err)
				c.disconnect()
				return
			}

			msg := c.parseMessage(message)

			select {
			case c.messageChan <- msg:
			case <-time.After(100 * time.Millisecond):
				log.Printf("[%s] Message channel blocked, dropping message", c.clientID)
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
					if k, ok := rawEvent["k"].(map[string]interface{}); ok {
						if interval, ok := k["i"].(string); ok {
							stream = strings.ToLower(symbol) + "@kline_" + interval
						}
					}
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
						c.mu.Lock()
						if c.isConnected && c.conn != nil {
							c.conn.WriteMessage(websocket.TextMessage, pong)
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

// DatabaseManager handles all QuestDB operations
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

// NewDatabaseManager creates a new QuestDB manager
func NewDatabaseManager(config *Config) (*DatabaseManager, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.QuestDBHost, config.QuestDBPort, config.QuestDBUser, config.QuestDBPass, config.QuestDBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to QuestDB: %v", err)
	}

	db.SetMaxOpenConns(config.MaxDBConnections)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping QuestDB: %v", err)
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
		return nil, fmt.Errorf("failed to initialize QuestDB tables: %v", err)
	}

	// Start background health check
	go dm.dbHealthCheck()

	return dm, nil
}

// dbHealthCheck periodically checks database connection
func (dm *DatabaseManager) dbHealthCheck() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			if err := dm.db.Ping(); err != nil {
				log.Printf("QuestDB connection unhealthy: %v", err)
			}
		}
	}
}

// initTables creates necessary QuestDB tables
func (dm *DatabaseManager) initTables() error {
	// Create trades table with designated timestamp
	tradeTable := `
	CREATE TABLE IF NOT EXISTS trades (
		trade_time TIMESTAMP,
		client_id SYMBOL,
		symbol SYMBOL,
		price DOUBLE,
		quantity DOUBLE,
		is_buyer_maker BOOLEAN,
		processed_at TIMESTAMP
	) TIMESTAMP(trade_time) PARTITION BY DAY;
	`

	// Create klines table with designated timestamp
	klineTable := `
	CREATE TABLE IF NOT EXISTS klines (
		open_time TIMESTAMP,
		client_id SYMBOL,
		symbol SYMBOL,
		interval SYMBOL,
		close_time TIMESTAMP,
		open_price DOUBLE,
		high_price DOUBLE,
		low_price DOUBLE,
		close_price DOUBLE,
		volume DOUBLE,
		is_closed BOOLEAN,
		processed_at TIMESTAMP
	) TIMESTAMP(open_time) PARTITION BY DAY;
	`

	if _, err := dm.db.Exec(tradeTable); err != nil {
		return fmt.Errorf("failed to create trades table: %v", err)
	}

	if _, err := dm.db.Exec(klineTable); err != nil {
		return fmt.Errorf("failed to create klines table: %v", err)
	}

	log.Println("QuestDB tables initialized")
	return nil
}

// Start begins the database processing workers
func (dm *DatabaseManager) Start() {
	log.Println("Starting QuestDB manager...")

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

// flushTrades writes accumulated trades to QuestDB
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

	// Start transaction
	tx, err := dm.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for trades: %v", err)
		return
	}
	defer tx.Rollback()

	// Prepare bulk insert
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO trades (
			trade_time, client_id, symbol, price, quantity, 
			is_buyer_maker, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		log.Printf("Failed to prepare trade statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute batch
	for _, trade := range batch {
		_, err := stmt.ExecContext(ctx,
			trade.TradeTime, trade.ClientID, trade.Symbol, trade.Price, trade.Quantity,
			trade.IsBuyerMaker, trade.ProcessedAt,
		)
		if err != nil {
			log.Printf("Failed to insert trade: %v", err)
			return
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit trades: %v", err)
		return
	}

	log.Printf("Inserted %d trades into QuestDB", len(batch))
}

// flushKlines writes accumulated klines to QuestDB
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

	// Start transaction
	tx, err := dm.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for klines: %v", err)
		return
	}
	defer tx.Rollback()

	// Prepare bulk insert
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO klines (
			open_time, client_id, symbol, interval, close_time,
			open_price, high_price, low_price, close_price, 
			volume, is_closed, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`)
	if err != nil {
		log.Printf("Failed to prepare kline statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute batch
	for _, kline := range batch {
		_, err := stmt.ExecContext(ctx,
			kline.OpenTime, kline.ClientID, kline.Symbol, kline.Interval, kline.CloseTime,
			kline.Open, kline.High, kline.Low, kline.Close,
			kline.Volume, kline.IsClosed, kline.ProcessedAt,
		)
		if err != nil {
			log.Printf("Failed to insert kline: %v", err)
			return
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit klines: %v", err)
		return
	}

	log.Printf("Inserted %d klines into QuestDB", len(batch))
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
	log.Println("Shutting down QuestDB manager...")
	dm.cancel()
	dm.wg.Wait()

	// Flush any remaining data
	dm.flushTrades()
	dm.flushKlines()

	if err := dm.db.Close(); err != nil {
		log.Printf("Error closing QuestDB connection: %v", err)
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

	// Calculate connections needed
	connectionsNeeded := (len(sm.symbols)*2 + sm.config.MaxStreamsPerConnection - 1) / sm.config.MaxStreamsPerConnection
	if connectionsNeeded < 1 {
		connectionsNeeded = 1
	}

	log.Printf("Creating %d WebSocket connections", connectionsNeeded)

	handler := sm.createMessageHandler()

	// Prepare stream lists
	spotStreams := make([][]string, connectionsNeeded)
	futuresStreams := make([][]string, connectionsNeeded)

	// Distribute streams across connections
	for i, symbol := range sm.symbols {
		connIndex := i % connectionsNeeded
		stream := strings.ToLower(symbol) + "@trade"
		spotStreams[connIndex] = append(spotStreams[connIndex], stream)
		futuresStreams[connIndex] = append(futuresStreams[connIndex], stream)
	}

	// Create and start clients
	for i := 0; i < connectionsNeeded; i++ {
		if len(spotStreams[i]) > 0 {
			spotClient := NewWebSocketClient(
				fmt.Sprintf("spot-%d", i),
				"wss://stream.binance.com:9443/stream",
				handler,
				sm.config.WorkerPoolSize,
			)

			if err := spotClient.Subscribe(spotStreams[i]...); err != nil {
				log.Printf("Failed to subscribe spot client %d: %v", i, err)
				continue
			}

			if err := spotClient.Start(); err != nil {
				log.Printf("Failed to start spot client %d: %v", i, err)
				continue
			}

			sm.clients = append(sm.clients, spotClient)
		}

		if len(futuresStreams[i]) > 0 {
			futuresClient := NewWebSocketClient(
				fmt.Sprintf("futures-%d", i),
				"wss://fstream.binance.com/stream",
				handler,
				sm.config.WorkerPoolSize,
			)

			if err := futuresClient.Subscribe(futuresStreams[i]...); err != nil {
				log.Printf("Failed to subscribe futures client %d: %v", i, err)
				continue
			}

			if err := futuresClient.Start(); err != nil {
				log.Printf("Failed to start futures client %d: %v", i, err)
				continue
			}

			sm.clients = append(sm.clients, futuresClient)
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

		now := time.Now().UTC()

		if strings.HasSuffix(msg.Stream, "@trade") {
			var trade TradeEvent
			if err := json.Unmarshal(msg.Data, &trade); err != nil {
				log.Printf("[%s] Trade unmarshal error: %v", msg.ClientID, err)
				return
			}

			price, err := strconv.ParseFloat(trade.Price, 64)
			if err != nil {
				log.Printf("[%s] Price parse error: %v", msg.ClientID, err)
				return
			}

			quantity, err := strconv.ParseFloat(trade.Quantity, 64)
			if err != nil {
				log.Printf("[%s] Quantity parse error: %v", msg.ClientID, err)
				return
			}

			tradeData := &TradeData{
				ClientID:     msg.ClientID,
				Symbol:       trade.Symbol,
				Price:        price,
				Quantity:     quantity,
				TradeTime:    time.Unix(0, trade.TradeTime*int64(time.Millisecond)).UTC(),
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

			open, err := strconv.ParseFloat(kline.Kline.Open, 64)
			if err != nil {
				log.Printf("[%s] Open price parse error: %v", msg.ClientID, err)
				return
			}

			high, err := strconv.ParseFloat(kline.Kline.High, 64)
			if err != nil {
				log.Printf("[%s] High price parse error: %v", msg.ClientID, err)
				return
			}

			low, err := strconv.ParseFloat(kline.Kline.Low, 64)
			if err != nil {
				log.Printf("[%s] Low price parse error: %v", msg.ClientID, err)
				return
			}

			close, err := strconv.ParseFloat(kline.Kline.Close, 64)
			if err != nil {
				log.Printf("[%s] Close price parse error: %v", msg.ClientID, err)
				return
			}

			volume, err := strconv.ParseFloat(kline.Kline.Volume, 64)
			if err != nil {
				log.Printf("[%s] Volume parse error: %v", msg.ClientID, err)
				return
			}

			klineData := &KlineData{
				ClientID:    msg.ClientID,
				Symbol:      kline.Symbol,
				Interval:    kline.Kline.Interval,
				OpenTime:    time.Unix(0, kline.Kline.StartTime*int64(time.Millisecond)).UTC(),
				CloseTime:   time.Unix(0, kline.Kline.CloseTime*int64(time.Millisecond)).UTC(),
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

	log.Println("Stream manager shut down")
}

// Statistics holds runtime statistics
type Statistics struct {
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
	atomic.AddInt64(&s.tradesProcessed, 1)
}

// IncrementKlines increments the kline counter
func (s *Statistics) IncrementKlines() {
	atomic.AddInt64(&s.klinesProcessed, 1)
}

// IncrementMessages increments the message counter
func (s *Statistics) IncrementMessages() {
	atomic.AddInt64(&s.messagesReceived, 1)
}

// GetStats returns current statistics
func (s *Statistics) GetStats() (int64, int64, int64, time.Duration) {
	return atomic.LoadInt64(&s.tradesProcessed),
		atomic.LoadInt64(&s.klinesProcessed),
		atomic.LoadInt64(&s.messagesReceived),
		time.Since(s.startTime)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	config := DefaultConfig()

	// Apply environment overrides
	if host := os.Getenv("QUESTDB_HOST"); host != "" {
		config.QuestDBHost = host
	}
	if portStr := os.Getenv("QUESTDB_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			config.QuestDBPort = port
		}
	}
	if user := os.Getenv("QUESTDB_USER"); user != "" {
		config.QuestDBUser = user
	}
	if pass := os.Getenv("QUESTDB_PASS"); pass != "" {
		config.QuestDBPass = pass
	}
	if name := os.Getenv("QUESTDB_NAME"); name != "" {
		config.QuestDBName = name
	}

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
		"SHIBUSDT", "MATICUSDT", "NEARUSDT", "ATOMUSDT", "LINKUSDT",
		"LTCUSDT", "BCHUSDT", "FILUSDT", "TRXUSDT", "ETCUSDT",
	}

	log.Printf("Starting crypto data collector for %d symbols", len(symbols))

	stats := NewStatistics()

	dbManager, err := NewDatabaseManager(config)
	if err != nil {
		log.Fatalf("Failed to initialize QuestDB manager: %v", err)
	}
	dbManager.Start()

	streamManager := NewStreamManager(config, dbManager, symbols)
	if err := streamManager.Start(); err != nil {
		log.Fatalf("Failed to start stream manager: %v", err)
	}

	// Statistics reporting
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				trades, klines, messages, uptime := stats.GetStats()
				log.Printf("STATS: Uptime=%v Messages=%d Trades=%d Klines=%d",
					uptime.Round(time.Second), messages, trades, klines)
			}
		}
	}()

	// Memory monitoring
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Printf("MEMORY: Alloc=%.2fMB Sys=%.2fMB NumGC=%d",
					float64(m.Alloc)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)
			}
		}
	}()

	log.Printf("System started: %d clients, %d workers",
		len(streamManager.clients), config.WorkerPoolSize)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Received %s, shutting down...", sig)

	startShutdown := time.Now()
	streamManager.Close()
	dbManager.Close()

	trades, klines, messages, uptime := stats.GetStats()
	log.Printf("FINAL STATS: Uptime=%v Messages=%d Trades=%d Klines=%d",
		uptime.Round(time.Second), messages, trades, klines)
	log.Printf("Shutdown completed in %v", time.Since(startShutdown))
}
