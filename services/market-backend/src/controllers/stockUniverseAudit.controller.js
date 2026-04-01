const { buildUniverseAudit, markFilteredStocksInactive } = require("../services/stockUniverseAudit.service");

const getStockUniverseAudit = async (_req, res) => {
  try {
    const audit = await buildUniverseAudit();
    return res.json({
      success: true,
      data: audit,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to build stock universe audit",
      error: err.message,
    });
  }
};

const markStockUniverseFilteredInactive = async (req, res) => {
  try {
    const result = await markFilteredStocksInactive(req.body || {});
    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to mark filtered stock rows inactive",
      error: err.message,
    });
  }
};

module.exports = {
  getStockUniverseAudit,
  markStockUniverseFilteredInactive,
};
