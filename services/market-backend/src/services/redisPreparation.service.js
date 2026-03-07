const redis = require("../config/redis.config");
const MarketState = require("../enums/marketState.enum");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const { getAllActiveStocks } = require("./activestock.service");

async function prepareRedisForMarket() {
  console.log("🟢 Preparing Redis for market...");

  const activeStocks = await getAllActiveStocks();

  if (!activeStocks.length) {
    throw new Error("No active stocks found");
  }

  // Store active symbols
  // store with symbol and exchange as key
  const symbols = activeStocks.map(
    (stock) => `${stock.symbol}#${stock.exchange}`,
  );
  await redis.set(REDIS_KEYS.STOCKS_ACTIVE, JSON.stringify(symbols));

  for (const stock of activeStocks) {
    // Map token
    await redis.set(`${REDIS_KEYS.STOCKS_TOKEN}${stock.symbol}`, stock.token);

    // Initialize live price object
    await redis.set(
      `${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`,
      JSON.stringify({
        symbol: stock.symbol,
        ltp: 0,
        open: 0,
        high: 0,
        low: 0,
        close: 0,
        volume: 0,
        percentChange: 0,
        avgPrice: 0,
        lowerCircuit: 0,
        upperCircuit: 0,
        week52Low: 0,
        week52High: 0,
        updatedAt: null,
        dayCandles: [],
        minuteBuffer: [],
        ltpBuffer: [],
      }),
    );
  }

  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.PREPARING);

  console.log(`✅ Redis prepared with ${symbols.length} stocks`);
}

module.exports = {
  prepareRedisForMarket,
};
