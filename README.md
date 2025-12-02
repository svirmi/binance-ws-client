# binance-ws-client

Binance websocket client. Features: connection/disconnection, ping/pong, graceful shutdown to clean up resources and avoid zombie connections, data-race-safe

Fixed by chatGPT [https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80](https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80)
Very interesing comments and advices (!!!) (like "I’ve seen many HFT-style pipelines hit this.") about PC resource consumption and connection issues: 
[https://chatgpt.com/g/g-p-692ddd7312e481918b5f440a494a8b08-binance-websocket/c/6837563e-c614-8002-b495-651a99066e80](https://chatgpt.com/g/g-p-692ddd7312e481918b5f440a494a8b08-binance-websocket/c/6837563e-c614-8002-b495-651a99066e80)

##### First start questdb docker container:

```
docker run -p 9000:9000 -p 8812:8812 questdb/questdb
```

#### Then ru the program with env variables:

```
QUESTDB_HOST=localhost QUESTDB_PORT=8812 go run main.go
```
