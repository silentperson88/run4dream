const activeStocksRepo = require("../repositories/activeStocks.repository");

exports.addStock = async (stockData, db) => {
  if (
    !stockData?.token ||
    !stockData?.exchange ||
    !stockData?.symbol ||
    !stockData?.master_id
  ) {
    throw new Error("Invalid stock data");
  }

  try {
    return await activeStocksRepo.create({ ...stockData, is_active: true }, db);
  } catch (err) {
    if (err.code === "23505") {
      throw new Error("Stock already active");
    }
    throw err;
  }
};

exports.getActiveStocks = async (page = 1, limit = 50, search = "") =>
  activeStocksRepo.listActive({ page, limit, search });

exports.getAllActiveStocks = async () => activeStocksRepo.listActive();

exports.deactivateStock = async (token) =>
  activeStocksRepo.updateByToken(token, { is_active: false });

exports.getActiveStockByToken = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) throw new Error("Active stock not found");
  return stock;
};

exports.getActiveStockByMasterId = async (masterId) =>
  activeStocksRepo.getByMasterId(masterId);

exports.updateActiveStockPrice = async (token, priceData) => {
  const updatedStock = await activeStocksRepo.updateByToken(token, {
    ltp: priceData.ltp,
    open: priceData.open,
    high: priceData.high,
    low: priceData.low,
    close: priceData.close,
    percentChange: priceData.percentChange,
    avgPrice: priceData.avgPrice,
    lowerCircuit: priceData.lowerCircuit,
    upperCircuit: priceData.upperCircuit,
    week52Low: priceData.week52Low,
    week52High: priceData.week52High,
    updatedAt: new Date(),
  });

  if (!updatedStock) throw new Error("Active stock not found");
  return updatedStock;
};

exports.bulkUpdateStocksInFullMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, [
    "ltp",
    "open",
    "high",
    "low",
    "close",
    "percentChange",
    "avgPrice",
    "lowerCircuit",
    "upperCircuit",
    "week52Low",
    "week52High",
  ]);
};

exports.bulkUpdateStocksInLTPMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, ["ltp"]);
};

exports.bulkUpdateStocksInOHLCMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, [
    "ltp",
    "open",
    "high",
    "low",
    "close",
  ]);
};

exports.toggleActiveStock = async (token) => {
  const stock = await activeStocksRepo.toggleByToken(token);
  if (!stock) throw new Error("Active stock not found");
  return stock;
};

exports.deleteActiveStock = async (token) => {
  const deleted = await activeStocksRepo.deleteByToken(token);
  if (!deleted) throw new Error("Active stock not found");
};

exports.getActiveStocksByMasterIds = async (masterIds = []) =>
  activeStocksRepo.listByMasterIds(masterIds);
