const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const activeStockService = require("../services/activestock.service");
const stockOhlcEodService = require("../services/stockOhlcEod.service");
const { normalizeEodDate } = require("../utils/Mthods.utils");

async function createEodFromRedis(req, res) {
  try {
    const { date } = req.body || {};
    const eodDate = normalizeEodDate(date || Date.now());

    const activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
    if (!activeStocksRaw) {
      return res.status(400).json({
        success: false,
        message: "No active stocks found in Redis",
      });
    }

    const activeStocks = await activeStockService.getAllActiveStocks();
    const symbolToId = {};
    for (const stock of activeStocks) {
      const key = `${stock.symbol}#${stock.exchange}`;
      symbolToId[key] = stock.master_id;
      if (!symbolToId[stock.symbol]) {
        symbolToId[stock.symbol] = stock.master_id;
      }
    }

    const symbols = JSON.parse(activeStocksRaw);
    const eodPayload = [];
    const seen = new Set();

    for (const symbol of symbols) {
      const baseSymbol = symbol.split("#")[0];
      const exchange = symbol.split("#")[1] || "NSE";
      const stockId = symbolToId[symbol] || symbolToId[baseSymbol];
      if (!stockId) continue;

      const dedupeKey = `${stockId}:${eodDate.toISOString()}`;
      if (seen.has(dedupeKey)) continue;

      const priceRaw = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${baseSymbol}`);
      if (!priceRaw) continue;

      const data = JSON.parse(priceRaw);
      if (!data) continue;

      eodPayload.push({
        master_id: stockId,
        symbol: baseSymbol,
        exchange,
        date: eodDate,
        open: data.open || 0,
        high: data.high || 0,
        low: data.low || 0,
        close: data.close || data.ltp || 0,
        volume: data.volume || 0,
      });

      seen.add(dedupeKey);
    }

    const saveResult = await stockOhlcEodService.createEodForAllStocks(eodPayload);

    return res.json({
      success: true,
      count: eodPayload.length,
      date: eodDate,
      insertedCount: saveResult?.insertedCount ?? null,
      writeErrors: saveResult?.writeErrors ?? 0,
      eodPayload,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to create EOD from Redis",
    });
  }
}

module.exports = {
  createEodFromRedis,
};
