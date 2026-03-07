const WebSocket = require("ws");
const { WEBSOKET_PORT } = process.env;
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const { toBaseSymbol, symbolCandidates } = require("./symbolChannel.util");

const wss = new WebSocket.Server({ port: WEBSOKET_PORT || 8080 });

// symbol -> Set of clients
const subscriptions = new Map();

wss.on("connection", (ws) => {
  console.log("Client connected");

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    const symbols = Array.isArray(msg.symbols) ? msg.symbols : [];

    // Subscribe symbols
    if (msg.type === "SUBSCRIBE") {
      console.log("Subscribing to:", symbols);
      symbols.forEach((symbol) => {
        const channel = toBaseSymbol(symbol);
        if (!channel) return;

        if (!subscriptions.has(channel)) {
          subscriptions.set(channel, new Set());
        }
        subscriptions.get(channel).add(ws);
      });

      // Immediately send latest snapshot for subscribed symbols
      symbols.forEach(async (symbol) => {
        try {
          const channel = toBaseSymbol(symbol);
          if (!channel) return;

          let price = null;
          for (const candidate of symbolCandidates(symbol)) {
            price = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${candidate}`);
            if (price) break;
          }
          if (!price) return;

          const payload = JSON.stringify({
            type: "PRICE_UPDATE",
            symbol: channel,
            data: JSON.parse(price),
          });

          if (ws.readyState === ws.OPEN) {
            ws.send(payload);
          }
        } catch (err) {
          console.error("Failed to send immediate price snapshot", err);
        }
      });
    }

    // Unsubscribe symbols
    if (msg.type === "UNSUBSCRIBE") {
      symbols.forEach((symbol) => {
        const channel = toBaseSymbol(symbol);
        subscriptions.get(channel)?.delete(ws);
      });
    }
  });

  ws.on("close", () => {
    subscriptions.forEach((clients) => clients.delete(ws));
    console.log("Client disconnected");
  });
});

module.exports = { wss, subscriptions };
