const { subscriptions } = require("./wsServer.service");
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const { toBaseSymbol, symbolCandidates } = require("./symbolChannel.util");

async function broadcastPrices(symbols) {
  console.log("Broadcasting prices...");

  for (const symbol of symbols) {
    const channel = toBaseSymbol(symbol);
    if (!channel) continue;

    const clients = subscriptions.get(channel);
    if (!clients || clients.size === 0) continue;

    let price = null;
    for (const candidate of symbolCandidates(symbol)) {
      price = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${candidate}`);
      if (price) break;
    }
    if (!price) continue;

    const payload = JSON.stringify({
      type: "PRICE_UPDATE",
      symbol: channel,
      data: JSON.parse(price),
    });

    clients.forEach((ws) => {
      if (ws.readyState === ws.OPEN) {
        ws.send(payload);
      }
    });
  }
}

module.exports = { broadcastPrices };
