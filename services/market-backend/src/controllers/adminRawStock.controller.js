const rawstockService = require("../services/rawstock.service");
const { SmartApiPriceService } = require("../services/smartapi.service");

const smartApiPriceService = new SmartApiPriceService();

const getAllRawStocks = async (req, res) => {
  try {
    const { page = 1, limit = 200, search = "" } = req.query;

    const result = await rawstockService.listRawStocks({
      page: Number(page),
      limit: Number(limit),
      search,
      exchanges: ["NSE", "BSE"],
    });

    return res.json({
      success: true,
      data: result.data,
      pagination: {
        page: result.page,
        limit: result.limit,
        total: result.total,
        totalPages: Math.ceil(result.total / result.limit),
      },
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Failed to fetch raw stocks",
      error: error.message,
    });
  }
};

const getRawStockPrice = async (req, res) => {
  try {
    let { mode, tokenIds, exchange } = req.body;

    if (!tokenIds) {
      return res.status(400).json({
        success: false,
        message: "TokenIds are required",
      });
    }

    mode = mode || "LTP";
    const data = await smartApiPriceService.getMarketData(mode, tokenIds, exchange);

    return res.json({ success: true, data });
  } catch (err) {
    return res.status(400).json({
      success: false,
      message: err.message,
    });
  }
};

const updateRawStockStatus = async (req, res) => {
  try {
    const { rawStockId, status } = req.body;

    if (!rawStockId || !status) {
      return res.status(400).json({
        success: false,
        message: "rawStockId and status are required",
      });
    }

    const allowedStatus = ["approved", "rejected"];
    if (!allowedStatus.includes(status)) {
      return res.status(400).json({
        success: false,
        message: "Invalid status value",
      });
    }

    const rawStock = await rawstockService.updateRawStock(rawStockId, { status });

    return res.status(200).json({
      success: true,
      data: {
        message:
          status === "approved"
            ? "Raw stock approved. Use master create API to add it to active stock."
            : `Raw stock marked as ${status}`,
        rawStock,
        activeStock: null,
      },
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
};

module.exports = { getAllRawStocks, getRawStockPrice, updateRawStockStatus };
