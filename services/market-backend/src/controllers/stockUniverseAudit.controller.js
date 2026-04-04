const {
  buildUniverseAudit,
  markFilteredStocksInactive,
  markStocksInactiveByActiveStockIds,
  addStockFromAuditRow,
  addStocksFromAuditRows,
} = require("../services/stockUniverseAudit.service");

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

const markStockUniverseRowsInactive = async (req, res) => {
  try {
    const activeStockIds = Array.isArray(req.body?.activeStockIds)
      ? req.body.activeStockIds
      : req.body?.activeStockId
        ? [req.body.activeStockId]
        : [];

    const result = await markStocksInactiveByActiveStockIds(activeStockIds);
    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to mark selected stock rows inactive",
      error: err.message,
    });
  }
};

const addStockFromAudit = async (req, res) => {
  try {
    const result = await addStockFromAuditRow(req.body || {});
    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to add stock from audit row",
      error: err.message,
    });
  }
};

const addStocksFromAudit = async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    const exchangeHint = req.body?.exchange || "";
    const result = await addStocksFromAuditRows(rows, exchangeHint);
    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to add audit rows in bulk",
      error: err.message,
    });
  }
};

module.exports = {
  getStockUniverseAudit,
  markStockUniverseFilteredInactive,
  markStockUniverseRowsInactive,
  addStockFromAudit,
  addStocksFromAudit,
};
