const {
  getUniverseRuleDefinitions,
  buildAndCacheEligibleUniverse,
  getHistoricalUniverseFilterCache,
  searchEligibleUniverse,
  searchEligibleUniverseUsingSplitData,
  searchEligibleUniverseUsingSplitDataFast,
} = require("../services/historicalUniverse.service");
const stockSearchService = require("../services/stockSearch.service");
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
    const result = await buildAndCacheEligibleUniverse({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      rules: payload.rules || {},
    });

    return res.json({
      success: true,
      data: {
        ...result.payload,
        cached: false,
        cache_key: result.cache?.cache_key || null,
      },
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to build historical universe",
      error: err.message,
    });
  }
};

const getHistoricalUniverseFilterCacheState = async (req, res) => {
  try {
    const payload = req.body || {};
    const cache = await getHistoricalUniverseFilterCache({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      rules: payload.rules || {},
    });

    return res.json({
      success: true,
      data: cache
        ? {
            cached: true,
            cache_key: cache.cache_key,
            cache_type: cache.cache_type,
            as_of_date: cache.as_of_date,
            rules_hash: cache.rules_hash,
            updated_at: cache.updated_at,
            result: cache.payload,
          }
        : {
            cached: false,
            result: null,
          },
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to load historical universe cache",
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
      historicalUniverseState: payload.historical_universe_state || null,
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

const searchHistoricalUniverseUsingSplitDataFast = async (req, res) => {
  try {
    const payload = req.body || {};
    const result = await stockSearchService.searchStocksUsingSplitDataFast({
      asOfDate: payload.as_of_date || payload.asOfDate || getAsOfDateFromRequest(req),
      query: String(payload.query || "").trim(),
      limit: Math.max(1, Math.min(100, Number(payload.limit || 50))),
      masterIds: Array.isArray(payload.master_ids) ? payload.master_ids : null,
    });

    return res.json({
      success: true,
      data: result,
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: "Failed to search historical universe with fast split snapshot",
      error: err.message,
    });
  }
};

module.exports = {
  getHistoricalUniverseRules,
  filterHistoricalUniverse,
  getHistoricalUniverseFilterCacheState,
  searchHistoricalUniverse,
  searchHistoricalUniverseUsingSplitData,
  searchHistoricalUniverseUsingSplitDataFast,
};
