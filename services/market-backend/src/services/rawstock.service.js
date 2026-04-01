const rawstocksRepo = require("../repositories/rawstocks.repository");
const { responseUtils } = require("../utils/Constants/responseContants.utils");

exports.getRawStockById = async (id) => {
  const stock = await rawstocksRepo.getById(id);
  if (!stock) throw new Error(responseUtils.RAW_STOCK_NOT_FOUND);
  return stock;
};

exports.createRawStock = async (data, db) => {
  const stock = await rawstocksRepo.create(data, db);
  if (!stock) throw new Error("Failed to create raw stock");
  return stock;
};

exports.updateRawStock = async (id, data, db) => {
  const stock = await rawstocksRepo.updateById(id, data, db);
  if (!stock) throw new Error(responseUtils.RAW_STOCK_NOT_FOUND);
  return stock;
};

exports.listRawStocks = async ({ page, limit, search, exchanges } = {}) =>
  rawstocksRepo.list({ page, limit, search, exchanges });
