const activeStockService = require("../services/activestock.service");
const constantsUtils = require("../utils/constants.utils");
const { response } = require("../utils/response.utils");
const { getAsOfDateFromRequest } = require("../utils/asOfDate.utils");

exports.getActiveStocksList = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit || req.query.pageSize, 10) || 50;
    const search = String(req.query.search || req.query.searchValue || "");
    const asOfDate = getAsOfDateFromRequest(req);

    const result = await activeStockService.getActiveStocks(page, limit, search, { asOfDate });

    return response(res, 200, "Active stocks fetched successfully", result);
  } catch (error) {
    return response(res, 500, constantsUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.getWatchlistSnapshot = async (req, res) => {
  try {
    const asOfDate = req.body?.as_of_date || getAsOfDateFromRequest(req) || null;
    const masterIds = Array.isArray(req.body?.master_ids) ? req.body.master_ids : [];
    const result = await activeStockService.getActiveStocksSnapshotByMasterIds(masterIds, {
      asOfDate,
    });

    return response(res, 200, "Watchlist snapshot fetched successfully", {
      as_of_date: asOfDate,
      count: result.length,
      rows: result,
    });
  } catch (error) {
    return response(res, 500, constantsUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.getBacktestAnalytics = async (req, res) => {
  try {
    const fromDate = req.body?.from_date || null;
    const toDate = req.body?.to_date || getAsOfDateFromRequest(req) || null;
    const masterIds = Array.isArray(req.body?.master_ids) ? req.body.master_ids : [];
    const result = await activeStockService.getBacktestAnalyticsByMasterIds(masterIds, {
      fromDate,
      toDate,
    });

    return response(res, 200, "Backtest analytics fetched successfully", {
      from_date: fromDate,
      to_date: toDate,
      count: result.length,
      rows: result,
    });
  } catch (error) {
    return response(res, 500, constantsUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.getActiveStockByToken = async (req, res) => {
  try {
    const stock = await activeStockService.getActiveStockByToken(req.params.token);
    res.status(200).json({ success: true, data: stock });
  } catch (error) {
    res.status(404).json({ success: false, message: error.message });
  }
};

exports.updateActiveStockPrice = async (req, res) => {
  try {
    const stock = await activeStockService.updateActiveStockPrice(
      req.params.token,
      req.body,
    );
    res.status(200).json({ success: true, data: stock });
  } catch (error) {
    res.status(400).json({ success: false, message: error.message });
  }
};

exports.toggleActiveStock = async (req, res) => {
  try {
    const stock = await activeStockService.toggleActiveStock(req.params.token);
    res.status(200).json({ success: true, data: stock });
  } catch (error) {
    res.status(400).json({ success: false, message: error.message });
  }
};

exports.deleteActiveStock = async (req, res) => {
  try {
    await activeStockService.deleteActiveStock(req.params.token);
    res.status(200).json({
      success: true,
      message: "Active stock removed successfully",
    });
  } catch (error) {
    res.status(400).json({ success: false, message: error.message });
  }
};
