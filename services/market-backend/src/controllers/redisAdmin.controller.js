const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const MarketState = require("../enums/marketState.enum");

function maybeParse(value) {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (!trimmed) return value;

  try {
    return JSON.parse(trimmed);
  } catch (_) {
    return value;
  }
}

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
      deleted += await redis.del(...keys);
    }
  } while (cursor !== "0");

  return deleted;
}

async function getRedisHealth(req, res) {
  try {
    const pong = await redis.ping();
    return res.json({
      success: true,
      data: {
        ping: pong,
        status: redis.status,
      },
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to check Redis health",
    });
  }
}

async function getRedisStatus(req, res) {
  try {
    const keys = [
      REDIS_KEYS.MARKET_STATE,
      REDIS_KEYS.MARKET_TIME,
      REDIS_KEYS.MARKET_DATE,
      REDIS_KEYS.MARKET_STARTED_AT,
      REDIS_KEYS.MARKET_LAST_RESET,
      REDIS_KEYS.MARKET_IS_RUNNING,
      REDIS_KEYS.MARKET_MINUTE_COUNTER,
      REDIS_KEYS.MARKET_LAST_CYCLE_AT,
      REDIS_KEYS.MARKET_FULL_DONE,
      REDIS_KEYS.MARKET_SCHEDULER_LOCK,
      REDIS_KEYS.MARKET_SCHEDULER_HEARTBEAT,
      "ltc:heartbeat",
      "ohlc:done",
    ];

    const pairs = await Promise.all(
      keys.map(async (key) => {
        const val = await redis.get(key);
        return [key, maybeParse(val)];
      }),
    );

    const data = Object.fromEntries(pairs);
    data.redisStatus = redis.status;

    return res.json({
      success: true,
      data,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to fetch Redis status",
    });
  }
}

async function listRedisKeys(req, res) {
  try {
    const pattern = String(req.query.pattern || "*");
    const limit = Math.min(Number(req.query.limit || 200), 1000);
    let cursor = "0";
    const keys = [];

    do {
      const [nextCursor, scanKeys] = await redis.scan(
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        500,
      );
      cursor = nextCursor;
      keys.push(...scanKeys);
    } while (cursor !== "0" && keys.length < limit);

    return res.json({
      success: true,
      count: Math.min(keys.length, limit),
      data: keys.slice(0, limit),
      pattern,
      limit,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to list Redis keys",
    });
  }
}

async function deleteRedisKeysByPattern(req, res) {
  try {
    const pattern = String(req.body?.pattern || "").trim();
    if (!pattern) {
      return res.status(422).json({
        success: false,
        message: "pattern is required",
      });
    }

    const deletedCount = await deleteByPattern(pattern);
    return res.json({
      success: true,
      message: "Keys deleted",
      pattern,
      deletedCount,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to delete Redis keys",
    });
  }
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
      REDIS_KEYS.MARKET_IS_RUNNING,
      REDIS_KEYS.MARKET_MINUTE_COUNTER,
      REDIS_KEYS.MARKET_LAST_CYCLE_AT,
      REDIS_KEYS.MARKET_FULL_DONE,
      REDIS_KEYS.FUNDAMENTALS_QUEUE,
      REDIS_KEYS.FUNDAMENTALS_DEDUPE,
      "ltc:heartbeat",
      "ohlc:done",
    ];

    deletedCount += await redis.del(...directKeys);
    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.INIT);

    return res.json({
      success: true,
      message: "Market Redis reset complete",
      deletedCount,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to reset market Redis",
    });
  }
}

async function flushRedisDb(req, res) {
  try {
    const result = await redis.flushdb();
    return res.json({
      success: true,
      message: "Redis DB flushed",
      result,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to flush Redis DB",
    });
  }
}

module.exports = {
  getRedisHealth,
  getRedisStatus,
  listRedisKeys,
  deleteRedisKeysByPattern,
  resetMarketRedis,
  flushRedisDb,
};
