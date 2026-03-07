const activeStockService = require("../services/activestock.service");
const constantsUtils = require("../utils/constants.utils");
const { response } = require("../utils/response.utils");

exports.getActiveStocksList = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit || req.query.pageSize, 10) || 50;
    const search = String(req.query.search || req.query.searchValue || "");

    const result = await activeStockService.getActiveStocks(page, limit, search);

    return response(res, 200, "Active stocks fetched successfully", result);
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
