package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsURL          = "wss://stream.binance.com/stream"
	workerCount    = 8
	rawChanBufSize = 5000
)

type RawWSMessage struct {
	Stream string
	Data   []byte
}

func main() {
	log.Println("Starting Binance WebSocket reader with worker pool...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ===== Create raw message channel (bidirectional) =====
	rawMsgChan := make(chan RawWSMessage, rawChanBufSize)

	// ===== Connect WebSocket =====
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("WebSocket dial failed: %v", err)
	}
	defer wsConn.Close()

	// ===== Subscribe to symbols =====
	subMsg := map[string]any{
		"method": "SUBSCRIBE",
		"params": []string{
			"btcusdt@depth20@100ms",
			"ethusdt@depth20@100ms",
			"bnbusdt@depth20@100ms",
			"xrpusdt@depth20@100ms",
			"adausdt@depth20@100ms",
			"solusdt@depth20@100ms",
			"dotusdt@depth20@100ms",
			"avaxusdt@depth20@100ms",
			"dogeusdt@depth20@100ms",
			"maticusdt@depth20@100ms",
		},
		"id": 1,
	}

	if err := wsConn.WriteJSON(subMsg); err != nil {
		log.Fatalf("Subscription failed: %v", err)
	}

	// ===== Start worker pool =====
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go worker(i, rawMsgChan, &wg)
	}

	// ===== Start WebSocket reader =====
	go wsReader(ctx, wsConn, rawMsgChan)

	// ===== Handle shutdown =====
	waitForExitSignal()
	cancel()
	close(rawMsgChan) // signal workers to stop
	wg.Wait()

	log.Println("Shutting down cleanly.")
}

//
// -------- WebSocket Reader --------
//

func wsReader(ctx context.Context, conn *websocket.Conn, out chan<- RawWSMessage) {
	conn.SetReadLimit(5 * 1024 * 1024) // safety limit

	conn.SetPongHandler(func(appData string) error {
		return conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	})

	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-pingTicker.C:
			conn.WriteMessage(websocket.PingMessage, nil)

		default:
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Println("WS read error:", err)
				return
			}

			// Non-blocking send (drops if full)
			select {
			case out <- RawWSMessage{Data: msg}:
			default:
				// Optional: report drop
				// log.Println("Raw message dropped: channel full")
			}
		}
	}
}

//
// -------- Worker Pool --------
//

func worker(id int, in <-chan RawWSMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range in {
		// Here you unmarshal JSON or push to Redis or hand off to other services
		// We just simulate some work:
		_ = msg // discard for now
		fmt.Printf("Worker %d processed message (%d bytes)\n", id, len(msg.Data))
	}
}

//
// -------- Exit Handling --------
//

func waitForExitSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
