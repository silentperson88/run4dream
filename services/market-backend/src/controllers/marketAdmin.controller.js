const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const MarketState = require("../enums/marketState.enum");

async function deleteByPattern(pattern) {
  let cursor = "0";
  let deleted = 0;

  do {
    const [nextCursor, keys] = await redis.scan(
      cursor,
      "MATCH",
      pattern,
      "COUNT",
      500,
    );
    cursor = nextCursor;

    if (keys.length) {
      await redis.del(...keys);
      deleted += keys.length;
    }
  } while (cursor !== "0");

  return deleted;
}

async function resetMarketRedis(req, res) {
  try {
    let deletedCount = 0;

    deletedCount += await deleteByPattern(`${REDIS_KEYS.STOCK_SNAPSHOT}*`);
    deletedCount += await deleteByPattern(`${REDIS_KEYS.STOCKS_TOKEN}*`);

    const directKeys = [
      REDIS_KEYS.STOCKS_ACTIVE,
      REDIS_KEYS.MARKET_STATE,
      REDIS_KEYS.MARKET_TIME,
      REDIS_KEYS.MARKET_DATE,
      REDIS_KEYS.MARKET_STARTED_AT,
      REDIS_KEYS.MARKET_LAST_RESET,
      REDIS_KEYS.MARKET_SCHEDULER_LOCK,
      REDIS_KEYS.MARKET_SCHEDULER_HEARTBEAT,
      "ltc:heartbeat",
      "ohlc:done",
    ];

    const removed = await redis.del(...directKeys);
    deletedCount += removed;

    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.INIT);

    return res.json({
      success: true,
      message: "Redis market data reset",
      deletedCount,
    });
  } catch (error) {
    console.error("Reset Redis Error:", error);
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to reset Redis",
    });
  }
}

module.exports = {
  resetMarketRedis,
};
