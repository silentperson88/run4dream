const fs = require("fs");
const path = require("path");
const redis = require("../config/redis.config");
const tokenService = require("../services/token.service");
const stockOhlcEodService = require("../services/stockOhlcEod.service");
const stockMasterService = require("../services/stockMaster.service");
const { REDIS_KEYS } = require("./Constants/redisKey.consants");

async function createEodFromSnapshot(snapshot) {
  const stocks = await stockMasterService.getAllMasterStocks();
  const symbolToId = {};
  for (const stock of stocks) {
    symbolToId[stock.symbol] = stock.id;
  }

  const eodPayload = [];
  for (const [symbol, data] of Object.entries(snapshot)) {
    const stockId = symbolToId[symbol.split("#")[0]];
    if (!stockId || !data) continue;

    eodPayload.push({ master_id: stockId, ...data });
  }

  await stockOhlcEodService.createEodForAllStocks(eodPayload);
}

async function generateMarketSnapshot() {
  const token = await tokenService.getLastEntry();
  if (!token) {
    throw new Error("No active token found");
  }

  const activeStocksRaw = await redis.get(REDIS_KEYS.STOCKS_ACTIVE);
  if (!activeStocksRaw) {
    throw new Error("No active stocks found in Redis");
  }

  const symbols = JSON.parse(activeStocksRaw);
  const stocksData = {};
  const marketDate = new Date().toISOString().slice(0, 10);

  for (const symbol of symbols) {
    const priceRaw = await redis.get(
      `${REDIS_KEYS.STOCK_SNAPSHOT}${symbol.split("#")[0]}`,
    );

    stocksData[symbol] = priceRaw
      ? { ...JSON.parse(priceRaw), date: marketDate }
      : null;
  }

  const snapshot = {
    meta: {
      date: new Date().toISOString().slice(0, 10),
      market_open: token.market.open_time,
      market_close: token.market.close_time,
      generated_at: new Date().toISOString(),
      scheduler_state: token.scheduler.state,
    },
    stocks: stocksData,
  };

  const fileName = `market_snapshot_${snapshot.meta.date}.json`;
  const filePath = path.join(process.cwd(), "snapshots", fileName);

  await createEodFromSnapshot(stocksData);

  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(snapshot, null, 2));

  return filePath;
}

module.exports = generateMarketSnapshot;
