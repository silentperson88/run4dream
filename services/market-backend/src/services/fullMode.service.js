const redis = require("../config/redis.config");
const MarketState = require("../enums/marketState.enum");
const MARKET_CONFIG = require("../config/market.config");
const delay = require("../utils/delay.util");
const createBatches = require("../utils/batch.util");
const { SmartApiPriceService } = require("./smartapi.service");
const { bulkUpdateStocksInFullMode } = require("./activestock.service");
const { prepareRedisForMarket } = require("./redisPreparation.service");
const { broadcastPrices } = require("../socket/broadcast");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");

const { FULL_MODE_BATCH_SIZE, API_BATCH_DELAY_MS } = MARKET_CONFIG;

const smartApiPriceService = new SmartApiPriceService();

function toBaseSymbol(symbolWithExchange) {
  return String(symbolWithExchange || "").split("#")[0];
}

function mapSmartApiFull(stock) {
  return {
    symbol: stock.tradingSymbol,
    token: stock.symbolToken,
    ltp: Number(stock.ltp) || 0,
    open: Number(stock.open) || 0,
    high: Number(stock.high) || 0,
    low: Number(stock.low) || 0,
    close: Number(stock.close) || 0,
    volume: Number(stock.volume) || 0,
    percentChange: Number(stock.percentChange) || 0,
    avgPrice: Number(stock.avgPrice) || 0,
    lowerCircuit: Number(stock.lowerCircuit) || 0,
    upperCircuit: Number(stock.upperCircuit) || 0,
    week52Low: Number(stock.week52Low || stock["52WeekLow"]) || 0,
    week52High: Number(stock.week52High || stock["52WeekHigh"]) || 0,
    dayCandles: [],
    updatedAt: new Date().toISOString(),
  };
}

async function fetchFullModeData(batchSymbols) {
  const mode = "FULL";

  const tokenList = await Promise.all(
    batchSymbols.map((symbol) =>
      redis.get(`${REDIS_KEYS.STOCKS_TOKEN}${toBaseSymbol(symbol)}`),
    ),
  );

  const validTokens = tokenList.filter(Boolean);
  if (!validTokens.length) {
    throw new Error("No token mappings found for FULL mode batch");
  }

  const result = await smartApiPriceService.getMarketData(mode, validTokens);
  if (!result || !result.status) {
    throw new Error("SmartAPI FULL mode request failed");
  }

  const fetched = Array.isArray(result?.data?.fetched) ? result.data.fetched : [];
  const unfetched = Array.isArray(result?.data?.unfetched) ? result.data.unfetched : [];

  if (!fetched.length) {
    console.warn(
      `FULL mode batch returned no fetched data. tokens=${validTokens.length}, unfetched=${unfetched.length}`,
    );
    if (unfetched.length) {
      console.warn(
        "FULL mode unfetched sample:",
        JSON.stringify(unfetched.slice(0, 3)),
      );
    }
    return [];
  }

  return fetched.map(mapSmartApiFull);
}

async function runFullMode() {
  console.log("FULL MODE started");
  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.FULL_RUNNING);

  let activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
  console.log("Active stocks for FULL mode:", activeStocksRaw);

  if (!activeStocksRaw) {
    console.warn(
      "No active stocks in Redis before FULL mode. Preparing Redis now...",
    );
    await prepareRedisForMarket();
    activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
  }

  if (!activeStocksRaw) {
    console.warn("No active stocks found for FULL mode");
    return;
  }

  const symbols = JSON.parse(activeStocksRaw);
  if (!Array.isArray(symbols) || !symbols.length) {
    console.warn("Active stocks list is empty for FULL mode");
    return;
  }

  const batches = createBatches(symbols, FULL_MODE_BATCH_SIZE);
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`FULL MODE batch ${i + 1}/${batches.length}`);

    let data = [];
    try {
      data = await fetchFullModeData(batch);
    } catch (error) {
      console.error(
        `FULL MODE batch ${i + 1}/${batches.length} failed:`,
        error.message,
      );
      if (i < batches.length - 1) {
        await delay(API_BATCH_DELAY_MS);
      }
      continue;
    }

    if (!data.length) {
      if (i < batches.length - 1) {
        await delay(API_BATCH_DELAY_MS);
      }
      continue;
    }

    for (const stock of data) {
      // console.log("Updating stock in FULL mode:", stock);
      await redis.set(
        `${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`,
        JSON.stringify(stock),
      );
      await broadcastPrices([stock.symbol]);
    }

    // Persist each successful batch immediately so DB is always updated
    await bulkUpdateStocksInFullMode(data);

    if (i < batches.length - 1) {
      await delay(API_BATCH_DELAY_MS);
    }
  }

  console.log("FULL MODE completed");
}

module.exports = {
  runFullMode,
};
