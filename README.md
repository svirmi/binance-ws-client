# binance-ws-client

Binance websocket client. Features: connection/disconnection, ping/pong, graceful shutdown to clean up resources and avoid zombie connections, data-race-safe

Fixed by chatGPT [https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80](https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80)

##### First start questdb docker container:

```
docker run -p 9000:9000 -p 8812:8812 questdb/questdb
```

#### Then ru the program with env variables:

```
QUESTDB_HOST=localhost QUESTDB_PORT=8812 go run main.go
```
