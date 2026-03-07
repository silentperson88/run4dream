const redis = require("../config/redis.config");
const MarketState = require("../enums/marketState.enum");
const MARKET_CONFIG = require("../config/market.config");
const delay = require("../utils/delay.util");
const createBatches = require("../utils/batch.util");
const { bulkUpdateStocksInOHLCMode } = require("./activestock.service");
const { SmartApiPriceService } = require("./smartapi.service");
const { broadcastPrices } = require("../socket/broadcast");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");

const { OHLC_MODE_BATCH_SIZE, API_BATCH_DELAY_MS } = MARKET_CONFIG;

const smartApiPriceService = new SmartApiPriceService();

function toBaseSymbol(symbolWithExchange) {
  return String(symbolWithExchange || "").split("#")[0];
}

function mapSmartApiOHLC(stock) {
  return {
    symbol: stock.tradingSymbol,
    token: stock.symbolToken,
    ltp: Number(stock.ltp) || 0,
    open: Number(stock.open) || 0,
    high: Number(stock.high) || 0,
    low: Number(stock.low) || 0,
    close: Number(stock.close) || 0,
    volume: Number(stock.volume) || 0,
    updatedAt: new Date().toISOString(),
  };
}

/**
 * 🔥 Replace with real OHLC API
 */
async function fetchOhlcModeData(batchSymbols) {
  // //   For developing mode as this api can't be hit multiple times in a minute
  // return [];

  const mode = "OHLC";

  // Wait for all token fetches to complete
  const tokenList = await Promise.all(
    batchSymbols.map((symbol) =>
      redis.get(`${REDIS_KEYS.STOCKS_TOKEN}${toBaseSymbol(symbol)}`),
    ),
  );
  const validTokens = tokenList.filter(Boolean);
  if (!validTokens.length) {
    throw new Error("No token mappings found for OHLC mode batch");
  }

  const result = await smartApiPriceService.getMarketData(mode, validTokens);

  if (!result || !result.status || result?.data?.fetched.length === 0) {
    throw new Error("No data received from SmartAPI FULL mode");
  }

  return result.data.fetched.map(mapSmartApiOHLC);
}

/**
 * 📊 OHLC MODE (Runs ONCE after market close)
 */
async function runOhlcMode(updateDb = false, updateNewCandle = false) {
  console.log("📊 OHLC MODE started");
  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.OHLC_RUNNING);

  const activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
  if (!activeStocksRaw) {
    console.warn("⚠ No active stocks found for OHLC");
    return;
  }

  const symbols = JSON.parse(activeStocksRaw);
  const batches = createBatches(symbols, OHLC_MODE_BATCH_SIZE);

  let allCycleData = []; // collect all batch data for DB update

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`📦 OHLC batch ${i + 1}/${batches.length}`);

    const data = await fetchOhlcModeData(batch);
    allCycleData.push(...data);

    for (const stock of data) {
      const redisKey = `${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`;
      const existingRaw = await redis.get(redisKey);

      if (!existingRaw) continue;

      const existing = JSON.parse(existingRaw);

      const dayCandles = existing.dayCandles || [];
      const minuteBuffer = existing.minuteBuffer || [];

      if (updateNewCandle) {
        const minuteCandle = {
          t: stock.updatedAt, // ISO timestamp of candle start
          o: stock.open,
          h: stock.high,
          l: stock.low,
          c: stock.close,
          v: stock.volume,
        };

        minuteBuffer.push(minuteCandle);

        if (minuteBuffer.length >= 5) {
          const five = minuteBuffer.slice(0, 5);
          const o = five[0].o;
          const c = five[4].c;
          let h = five[0].h;
          let l = five[0].l;
          let v = 0;
          for (const m of five) {
            if (m.h > h) h = m.h;
            if (m.l < l) l = m.l;
            v += m.v || 0;
          }

          dayCandles.push({
            t: five[0].t,
            o,
            h,
            l,
            c,
            v,
          });

          minuteBuffer.splice(0, 5);
        }
      }

      const updated = {
        ...existing,
        ltp: stock.ltp, // update LTP as well,
        open: stock.open,
        high: stock.high,
        low: stock.low,
        close: stock.close,
        volume: stock.volume,
        lastUpdated: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        dayCandles,
        minuteBuffer,
      };

      await redis.set(redisKey, JSON.stringify(updated));

      await broadcastPrices([stock.symbol]);
    }

    if (i < batches.length - 1) {
      await delay(API_BATCH_DELAY_MS);
    }
  }

  // 🔹 Bulk update DB once per cycle after all batches
  if (updateDb) await bulkUpdateStocksInOHLCMode(allCycleData);

  console.log("🟢 OHLC MODE completed");
  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.OHLC_DONE);
}

module.exports = {
  runOhlcMode,
};
