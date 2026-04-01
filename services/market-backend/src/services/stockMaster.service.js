const stockMasterRepo = require("../repositories/stockMaster.repository");
const { responseUtils } = require("../utils/Constants/responseContants.utils");

exports.createMasterStock = async (stockData, db) => {
  if (
    !stockData?.token ||
    !stockData?.exchange ||
    !stockData?.symbol ||
    !(stockData?.raw_stock_id || stockData?.rawStockId)
  ) {
    throw new Error(responseUtils.INVALID_STATUS_VALUE);
  }

  try {
    return await stockMasterRepo.create(stockData, db);
  } catch (err) {
    if (err.code === "23505") {
      throw new Error(responseUtils.STOCK_ALREADY_EXISTS);
    }
    throw err;
  }
};

exports.updateMasterStock = async (masterId, data) =>
  stockMasterRepo.updateById(masterId, data);

exports.getMasterList = async (page = 1, limit = 50, query = {}) => {
  const search = query?.search || "";
  const is_active = query?.is_active ?? query?.isActive ?? true;
  const result = await stockMasterRepo.list({ page, limit, search, is_active });
  return result.data;
};

exports.deactivateStock = async (token) => {
  const stock = await stockMasterRepo.getByToken(token);
  if (!stock) return null;
  return stockMasterRepo.updateById(stock.id, { is_active: false });
};

exports.getMasterStockByToken = async (token) => {
  const stock = await stockMasterRepo.getByToken(token);
  if (!stock) throw new Error(responseUtils.ACTIVE_STOCK_NOT_FOUND);
  return stock;
};

exports.getMasterStockById = async (id) => stockMasterRepo.getById(id);

exports.getMasterStockBySymbol = async (symbol) =>
  stockMasterRepo.getBySymbolOrName(symbol);

exports.getMasterStockByName = async (name) => stockMasterRepo.getByName(name);

exports.getAllMasterStocks = async () => stockMasterRepo.listActive();

exports.canFetchScreener = (stock) => {
  if (!stock) return false;
  return (
    stock.is_active === true &&
    Boolean(String(stock.screener_url || "").trim()) &&
    String(stock.screener_status || "PENDING").toUpperCase() === "PENDING"
  );
};

exports.syncCompanyFromFundamental = async () => stockMasterRepo.syncCompanyFromFundamentals();

exports.setFetchCount = async (id, count = 1, db) =>
  stockMasterRepo.setFetchCount(id, count, db);

exports.updateHistoryCoverage = async (id, payload = {}, db) =>
  stockMasterRepo.updateHistoryCoverage(id, payload, db);
