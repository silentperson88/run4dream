const activeStocksRepo = require("../repositories/activeStocks.repository");
const stockMasterRepo = require("../repositories/stockMaster.repository");

exports.addStock = async (stockData, db) => {
  if (
    !stockData?.exchange ||
    !stockData?.symbol ||
    !stockData?.master_id
  ) {
    throw new Error("Invalid stock data");
  }

  try {
    return await activeStocksRepo.create(stockData, db);
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

exports.deactivateStock = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) return null;
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master) return null;
  return stockMasterRepo.updateById(master.id, { is_active: false });
};

exports.getActiveStockByToken = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) throw new Error("Active stock not found");
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master?.is_active) throw new Error("Active stock not found");
  return stock;
};

exports.getActiveStockByMasterId = async (masterId) => {
  const stock = await activeStocksRepo.getByMasterId(masterId);
  if (!stock) return null;
  const master = await stockMasterRepo.getById(masterId);
  if (!master?.is_active) return null;
  return stock;
};

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
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) throw new Error("Active stock not found");
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master) throw new Error("Active stock not found");

  const updatedMaster = await stockMasterRepo.updateById(master.id, {
    is_active: !Boolean(master.is_active),
  });

  if (!updatedMaster) throw new Error("Active stock not found");

  return {
    ...stock,
    master_is_active: updatedMaster.is_active,
  };
};

exports.deleteActiveStock = async (token) => {
  const deleted = await activeStocksRepo.deleteByToken(token);
  if (!deleted) throw new Error("Active stock not found");
};

exports.getActiveStocksByMasterIds = async (masterIds = []) =>
  activeStocksRepo.listByMasterIds(masterIds);
