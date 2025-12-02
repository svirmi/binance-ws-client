// main.go
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

// ===== CONFIG =====
const (
	// Binance futures websocket (use fstream for futures)
	binanceWSURL = "wss://fstream.binance.com/ws"

	// Worker pool size (tune to number of CPU cores)
	workerCount = 8

	// Buffered channel size for raw messages (tune for memory vs throughput)
	rawChanBufSize = 65536

	// Read deadline (extended on pong). Keep > expected ping interval.
	readDeadline = 90 * time.Second

	// Ping interval (Binance recommends ~3 minutes; choose smaller to be safe)
	pingInterval = 3 * time.Minute
)

// ===== MESSAGE TYPES =====
// BookTicker format (see Binance bookTicker)
type BookTicker struct {
	EventType string `json:"e"` // "bookTicker"
	EventTs   int64  `json:"E"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidQty    string `json:"B"`
	AskPrice  string `json:"a"`
	AskQty    string `json:"A"`
	TransTs   int64  `json:"T"`
}

// Combined stream wrapper: {"stream":"...","data":{...}}
type CombinedMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// Raw message pushed from reader to workers
type RawMsg struct {
	Data []byte
}

// ===== CLIENT STRUCT =====
type WSClient struct {
	url     string
	symbols []string

	rawCh chan RawMsg

	// lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	isRunning atomic.Bool
	msgCount  atomic.Int64
}

func NewWSClient(url string, symbols []string) *WSClient {
	ctx, cancel := context.WithCancel(context.Background())

	// normalize symbols to lowercase for subscription
	ll := make([]string, len(symbols))
	for i, s := range symbols {
		ll[i] = strings.ToLower(s)
	}

	return &WSClient{
		url:     url,
		symbols: ll,
		rawCh:   make(chan RawMsg, rawChanBufSize),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins the client (worker pool + connection runloop)
func (c *WSClient) Start() error {
	if !c.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("already running")
	}

	// start workers
	for i := 0; i < workerCount; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}

	// connection manager (reconnect loop)
	c.wg.Add(1)
	go c.run()

	return nil
}

func (c *WSClient) Stop() {
	if !c.isRunning.CompareAndSwap(true, false) {
		return
	}
	log.Println("[client] stopping...")
	c.cancel()
	// close raw channel after workers exit: we close it in run() when stopping
	c.wg.Wait()
	log.Printf("[client] stopped. messages processed: %d\n", c.msgCount.Load())
}

// run manages connect → read → heartbeat → reconnect
func (c *WSClient) run() {
	defer c.wg.Done()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		// exit if parent context canceled
		select {
		case <-c.ctx.Done():
			// close raw channel to signal workers to stop
			close(c.rawCh)
			return
		default:
		}

		// dial
		conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
		if err != nil {
			log.Printf("[run] dial error: %v (backoff %v)", err, backoff)
			time.Sleep(backoff + jitter(backoff))
			backoff = clampDuration(backoff*2, maxBackoff)
			continue
		}

		// set handlers and deadline
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		conn.SetPongHandler(func(appData string) error {
			_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
			// optionally log: log.Printf("[pong] %s", appData)
			return nil
		})
		conn.SetPingHandler(func(appData string) error {
			// Reply with pong control frame
			deadline := time.Now().Add(5 * time.Second)
			if err := conn.WriteControl(websocket.PongMessage, []byte(appData), deadline); err != nil {
				log.Printf("[pingHandler] write pong error: %v", err)
				return err
			}
			_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
			return nil
		})

		// subscribe to bookTicker streams
		params := make([]string, 0, len(c.symbols))
		for _, s := range c.symbols {
			params = append(params, fmt.Sprintf("%s@bookTicker", s))
		}
		subMsg := map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     1,
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			log.Printf("[run] subscribe error: %v", err)
			_ = conn.Close()
			time.Sleep(backoff + jitter(backoff))
			backoff = clampDuration(backoff*2, maxBackoff)
			continue
		}

		// reset backoff on connection success
		backoff = time.Second
		log.Println("[run] connected and subscribed")

		// per-connection context
		connCtx, connCancel := context.WithCancel(c.ctx)

		// start heartbeat goroutine (sends Ping control frames periodically)
		var connWG sync.WaitGroup
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			ticker := time.NewTicker(pingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-connCtx.Done():
					return
				case <-ticker.C:
					deadline := time.Now().Add(5 * time.Second)
					payload := []byte(fmt.Sprintf("%d", time.Now().UnixMilli()))
					if err := conn.WriteControl(websocket.PingMessage, payload, deadline); err != nil {
						log.Printf("[heartbeat] failed to send ping: %v", err)
						connCancel()
						return
					}
				}
			}
		}()

		// start reader loop (fast, non-blocking)
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			for {
				// prefer exiting quickly
				select {
				case <-connCtx.Done():
					return
				default:
				}

				// set read deadline; pong handler extends it
				_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
				_, msg, err := conn.ReadMessage()
				if err != nil {
					// log and trigger reconnect
					log.Printf("[reader] read error: %v", err)
					connCancel()
					return
				}

				// non-blocking push into raw channel
				select {
				case c.rawCh <- RawMsg{Data: msg}:
				default:
					// channel full: drop oldest then push (keep-latest policy)
					select {
					case <-c.rawCh:
						// dropped oldest
					default:
					}
					// try push again (non-blocking)
					select {
					case c.rawCh <- RawMsg{Data: msg}:
					default:
						// If still can't push, drop silently
					}
				}
			}
		}()

		// wait until reader or heartbeat calls connCancel()
		connWG.Wait()

		// close connection and continue to reconnect
		_ = conn.Close()

		// small sleep before reconnect to avoid hot loop (backoff applies)
		// check if top-level ctx canceled
		if c.ctx.Err() != nil {
			// close raw channel to signal workers to stop
			close(c.rawCh)
			return
		}

		// continue loop (it will backoff on next dial error)
		log.Println("[run] connection ended, reconnecting...")
		// slight pause to avoid immediate reconnection storms
		time.Sleep(200 * time.Millisecond)
		connCancel()
	}
}

// worker consumes raw messages and unmarshals JSON
func (c *WSClient) worker(id int) {
	defer c.wg.Done()
	log.Printf("[worker %02d] started\n", id)

	for msg := range c.rawCh {
		// message can be either direct bookTicker JSON or combined wrapper
		// try combined first
		var comb CombinedMessage
		if err := json.Unmarshal(msg.Data, &comb); err == nil && comb.Stream != "" {
			// parse comb.Data as BookTicker
			var bt BookTicker
			if err := json.Unmarshal(comb.Data, &bt); err == nil && bt.EventType == "bookTicker" {
				c.handleBookTicker(bt)
				continue
			}
		}

		// fallback: try direct BookTicker
		var bt BookTicker
		if err := json.Unmarshal(msg.Data, &bt); err == nil && bt.EventType == "bookTicker" {
			c.handleBookTicker(bt)
			continue
		}

		// other message types ignored (subscription ack etc.)
		// optionally, could parse control messages here
	}
	log.Printf("[worker %02d] stopped\n", id)
}

// handleBookTicker is where you process best bid/ask (currently logs)
func (c *WSClient) handleBookTicker(bt BookTicker) {
	// Example: log best bid/ask and increment counter
	c.msgCount.Add(1)

	// Timestamp handling: prefer EventTs (E) else T
	var ts time.Time
	if bt.EventTs > 0 {
		ts = time.Unix(0, bt.EventTs*int64(time.Millisecond))
	} else if bt.TransTs > 0 {
		ts = time.Unix(0, bt.TransTs*int64(time.Millisecond))
	} else {
		ts = time.Now()
	}

	// Replace this with your processing: store snapshot, push to aggregator, etc.
	log.Printf("%s | Bid: %s/%s | Ask: %s/%s | %s",
		strings.ToUpper(bt.Symbol),
		bt.BidPrice, bt.BidQty,
		bt.AskPrice, bt.AskQty,
		ts.Format("15:04:05.000"))
}

// ===== Utilities =====
func jitter(d time.Duration) time.Duration {
	// random jitter up to 50% of d
	return time.Duration(rand.Int63n(int64(d) / 2))
}

func clampDuration(d, max time.Duration) time.Duration {
	if d > max {
		return max
	}
	return d
}

// ===== MAIN =====
func main() {
	// seed rand
	rand.Seed(time.Now().UnixNano())

	// symbols list (example)
	symbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
		"SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "LUNAUSDT",
	}

	client := NewWSClient(binanceWSURL, symbols)

	if err := client.Start(); err != nil {
		log.Fatalf("failed to start client: %v", err)
	}

	// stats logger
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-client.ctx.Done():
				return
			case <-statsTicker.C:
				log.Printf("[stats] messages processed: %d\n", client.msgCount.Load())
			}
		}
	}()

	// wait for termination signal
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
	<-sigch

	// stop
	client.Stop()
}
