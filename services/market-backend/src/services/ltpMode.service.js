const redis = require("../config/redis.config");
const MarketState = require("../enums/marketState.enum");
const MARKET_CONFIG = require("../config/market.config");
const delay = require("../utils/delay.util");
const createBatches = require("../utils/batch.util");
const { SmartApiPriceService } = require("./smartapi.service");
const { bulkUpdateStocksInLTPMode } = require("./activestock.service");
const { isMarketClosed } = require("../utils/Mthods.utils");
const { broadcastPrices } = require("../socket/broadcast");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");

const {
  LTC_MODE_BATCH_SIZE,
  API_BATCH_DELAY_MS,
  LTC_CYCLE_DELAY_MS,
  MARKET_CLOSE_TIME,
} = MARKET_CONFIG;

const smartApiPriceService = new SmartApiPriceService();

function toBaseSymbol(symbolWithExchange) {
  return String(symbolWithExchange || "").split("#")[0];
}

function mapSmartApiLTP(stock) {
  return {
    symbol: stock.tradingSymbol,
    token: stock.symbolToken,
    ltp: Number(stock.ltp) || 0,
    // // random LTP for testing purpose
    // ltp: Math.floor(Math.random() * 1000),
    updatedAt: new Date().toISOString(),
  };
}

/**
 * 🔥 Replace with real LTP API
 */
async function fetchLtcModeData(batchSymbols) {
  //   For developing mode as this api can't be hit multiple times in a minute
  // const randomArray = batchSymbols.map((symbol) => {
  //   return {
  //     symbol: symbol,
  //     token: symbol,
  //     // ltp: Number(stock.ltp) || 0,
  //     // random LTP for testing purpose
  //     ltp: Math.floor(Math.random() * 1000),
  //     updatedAt: new Date().toISOString(),
  //   };
  // });
  // return randomArray;

  const mode = "LTP";

  // Wait for all token fetches to complete
  const tokenList = await Promise.all(
    batchSymbols.map((symbol) =>
      redis.get(`${REDIS_KEYS.STOCKS_TOKEN}${toBaseSymbol(symbol)}`),
    )
  );
  const validTokens = tokenList.filter(Boolean);
  if (!validTokens.length) {
    throw new Error("No token mappings found for LTP mode batch");
  }

  const result = await smartApiPriceService.getMarketData(mode, validTokens);

  if (!result || !result.status || result?.data?.fetched.length === 0) {
    throw new Error("No data received from SmartAPI FULL mode");
  }

  return result.data.fetched.map(mapSmartApiLTP);
}

/**
 * LTP MODE LOOP
 */
async function runLtcMode() {
  console.log("🟡 LTP MODE started");
  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.LTC_RUNNING);

  while (true) {
    if (await isMarketClosed()) {
      console.log("🔴 Market closed, stopping LTP mode");
      await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.MARKET_CLOSED);
      break;
    }

    const activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
    if (!activeStocksRaw) {
      console.warn("⚠ No active stocks found for OHLC");
      return;
    }

    const symbols = JSON.parse(activeStocksRaw);
    const batches = createBatches(symbols, LTC_MODE_BATCH_SIZE);

    // Process each batch
    for (let i = 0; i < batches.length; i++) {
      if (await isMarketClosed()) {
        console.log("🔴 Market closed mid-cycle");
        await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.MARKET_CLOSED);
        return;
      }

      const batch = batches[i];
      console.log(`⚡ LTP batch ${i + 1}/${batches.length}`);

      const data = await fetchLtcModeData(batch);

      for (const stock of data) {
        const existingRaw = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`);
        if (!existingRaw) continue;

        const existing = JSON.parse(existingRaw);

        const updated = {
          ...existing,
          ltp: stock.ltp,
          updatedAt: stock.updatedAt,
        };

        await redis.set(
          `${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`,
          JSON.stringify(updated)
        );
      }

      // 🔹 Bulk update DB after Redis is updated in batch
      await bulkUpdateStocksInLTPMode(data);

      if (i < batches.length - 1) {
        await delay(API_BATCH_DELAY_MS);
      }
    }

    console.log(
      `⏳ LTP batch cycle completed, next batch will start after ${
        LTC_CYCLE_DELAY_MS / 1000 / 60
      } minutes`
    );
    await delay(LTC_CYCLE_DELAY_MS);
  }

  console.log("🟢 LTP MODE stopped");
}

// LTP 2nd Mode that runs every minute during market hours
async function runLtcBasicMode({
  setState = true,
  broadcast = true,
  updateDb = false,
} = {}) {
  console.log("LTP MODE started");
  if (setState) {
    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.LTC_RUNNING);
  }

  const activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
  if (!activeStocksRaw) {
    console.warn("No active stocks found for LTP");
    return;
  }

  const symbols = JSON.parse(activeStocksRaw);
  const batches = createBatches(symbols, LTC_MODE_BATCH_SIZE);
  console.log(
    `LTP cycle started: symbols=${symbols.length}, batches=${batches.length}, batchSize=${LTC_MODE_BATCH_SIZE}`
  );

  // Collect all batch results for optional DB update
  let allCycleData = [];

  for (let i = 0; i < batches.length; i++) {
    if (await isMarketClosed()) {
      console.log("Market closed mid-cycle");
      if (setState) {
        await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.MARKET_CLOSED);
      }
      return;
    }

    const batch = batches[i];
    console.log(`LTP batch ${i + 1}/${batches.length}`);

    const data = await fetchLtcModeData(batch);
    allCycleData.push(...data);

    // Update Redis per stock in batch
    for (const stock of data) {
      const redisKey = `${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`;
      const existingRaw = await redis.get(redisKey);

      const existing = existingRaw
        ? JSON.parse(existingRaw)
        : { symbol: stock.symbol, ltp: 0, lastUpdated: null, dayCandles: [] };

      const dayCandles = existing.dayCandles || [];
      const ltpBuffer = existing.ltpBuffer || [];

      ltpBuffer.push({
        t: stock.updatedAt,
        p: stock.ltp,
      });

      if (ltpBuffer.length >= 5) {
        const five = ltpBuffer.slice(0, 5);
        const o = five[0].p;
        const c = five[4].p;
        let h = five[0].p;
        let l = five[0].p;
        for (const m of five) {
          if (m.p > h) h = m.p;
          if (m.p < l) l = m.p;
        }

        dayCandles.push({
          t: five[0].t,
          o,
          h,
          l,
          c,
          v: 0,
        });

        ltpBuffer.splice(0, 5);
      }

      const open = existing.open ? existing.open : stock.ltp;
      const high =
        typeof existing.high === "number" && existing.high !== 0
          ? Math.max(existing.high, stock.ltp)
          : stock.ltp;
      const low =
        typeof existing.low === "number" && existing.low !== 0
          ? Math.min(existing.low, stock.ltp)
          : stock.ltp;
      const close = stock.ltp;

      const updated = {
        ...existing,
        ltp: stock.ltp,
        open,
        high,
        low,
        close,
        lastUpdated: stock.updatedAt,
        updatedAt: stock.updatedAt,
        dayCandles,
        ltpBuffer,
      };

      await redis.set(redisKey, JSON.stringify(updated));
      // console.log("Updated Redis for stock:", stock, updated);

      if (broadcast) {
        await broadcastPrices([stock.symbol]);
      }
    }

    if (i < batches.length - 1) {
      await delay(API_BATCH_DELAY_MS);
    }
  }

  // Bulk update DB after all batches (optional)
  if (updateDb) {
    await bulkUpdateStocksInLTPMode(allCycleData);
  }

  console.log("LTP MODE stopped");
  if (setState) {
    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.MARKET_CLOSED);
  }
}

module.exports = {
  runLtcMode,
  runLtcBasicMode,
};



