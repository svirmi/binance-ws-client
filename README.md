# binance-ws-client

Binance websocket client. Features: connection/disconnection, ping/pong, graceful shutdown to clean up resources and avoid zombie connections, data-race-safe

Fixed by chatGPT [https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80](https://chatgpt.com/c/6837563e-c614-8002-b495-651a99066e80)
Very interesing comments and advices (!!!) (like "I’ve seen many HFT-style pipelines hit this.") about PC resource consumption and connection issues: 
[https://chatgpt.com/g/g-p-692ddd7312e481918b5f440a494a8b08-binance-websocket/c/6837563e-c614-8002-b495-651a99066e80](https://chatgpt.com/g/g-p-692ddd7312e481918b5f440a494a8b08-binance-websocket/c/6837563e-c614-8002-b495-651a99066e80)

Send normalized messages to Redis (https://claude.ai/chat/2c9f9dd5-f439-4a40-b904-2dc65c1794ec)[https://claude.ai/chat/2c9f9dd5-f439-4a40-b904-2dc65c1794ec]

Connection Sharding by Gemini (https://gemini.google.com/app/c8cf005ca0b7fea0#1cb114599efdc6ae)[https://gemini.google.com/app/c8cf005ca0b7fea0#1cb114599efdc6ae]
