const {
  getUniverseRuleDefinitions,
  buildEligibleUniverse,
  searchEligibleUniverse,
  searchEligibleUniverseUsingSplitData,
} = require("../services/historicalUniverse.service");
const { getAsOfDateFromRequest } = require("../utils/asOfDate.utils");

const getHistoricalUniverseRules = async (_req, res) => {
  try {
    return res.json({
      success: true,
      data: getUniverseRuleDefinitions(),
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to load historical universe rules",
      error: err.message,
    });
  }
};

const filterHistoricalUniverse = async (req, res) => {
  try {
    const payload = req.body || {};
    const result = await buildEligibleUniverse({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      rules: payload.rules || {},
    });

    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to build historical universe",
      error: err.message,
    });
  }
};

const searchHistoricalUniverse = async (req, res) => {
  try {
    const payload = req.body || {};
    const result = await searchEligibleUniverse({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      rules: payload.rules || {},
      query: String(payload.query || "").trim(),
      limit: Math.max(1, Math.min(100, Number(payload.limit || 50))),
      masterIds: Array.isArray(payload.master_ids) ? payload.master_ids : null,
      universeSummary: payload.universe || null,
    });

    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to search historical universe",
      error: err.message,
    });
  }
};

const searchHistoricalUniverseUsingSplitData = async (req, res) => {
  try {
    const payload = req.body || {};
    const result = await searchEligibleUniverseUsingSplitData({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      rules: payload.rules || {},
      query: String(payload.query || "").trim(),
      limit: Math.max(1, Math.min(100, Number(payload.limit || 50))),
      masterIds: Array.isArray(payload.master_ids) ? payload.master_ids : null,
      universeSummary: payload.universe || null,
    });

    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to search historical universe with split fundamentals",
      error: err.message,
    });
  }
};

module.exports = {
  getHistoricalUniverseRules,
  filterHistoricalUniverse,
  searchHistoricalUniverse,
  searchHistoricalUniverseUsingSplitData,
};
