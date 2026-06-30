const { normalizeAsOfDate } = require("../utils/asOfDate.utils");
const stockMasterService = require("./stockMaster.service");
const eodRepo = require("../repositories/eod.repository");
const stockSearchService = require("./stockSearch.service");
const historicalUniverseCacheRepo = require("../repositories/historicalUniverseCache.repository");

const RULE_DEFINITIONS = {
  has_min_history: {
    id: "has_min_history",
    label: "Has minimum history",
    description: "Keep stocks that have at least the configured number of EOD candles before the as-of date.",
    defaultEnabled: true,
    parameters: {
      minCandles: 60,
    },
  },
  recent_data_available: {
    id: "recent_data_available",
    label: "Recent trading continuity",
    description: "Keep stocks whose last few trade dates stay within a tight calendar span.",
    defaultEnabled: true,
    parameters: {
      recentEntries: 4,
      maxGapDays: 5,
    },
  },
  recent_trades_in_window: {
    id: "recent_trades_in_window",
    label: "Recent trade activity",
    description: "Keep stocks that have at least the configured number of trades in the last N calendar days ending on the as-of date.",
    defaultEnabled: false,
    parameters: {
      lookbackDays: 28,
      minTrades: 5,
    },
  },
  zero_volume_last_5d: {
    id: "zero_volume_last_5d",
    label: "No zero-volume in last 5 trading days",
    description: "Exclude stocks if any candle in the recent trading window has zero volume.",
    defaultEnabled: true,
    parameters: {
      days: 5,
    },
  },
};

const UNIVERSE_STATE_VERSION = "v5";

const sanitizePositiveInteger = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.floor(parsed));
};

const buildRuleConfig = (rules = {}) => {
  return Object.values(RULE_DEFINITIONS).reduce((acc, definition) => {
    const incoming = rules?.[definition.id] || {};
    const enabled = incoming.enabled === undefined ? definition.defaultEnabled : Boolean(incoming.enabled);
    const parameters = Object.entries(definition.parameters || {}).reduce((paramAcc, [key, fallback]) => {
      paramAcc[key] = sanitizePositiveInteger(incoming?.[key], fallback);
      return paramAcc;
    }, {});

    acc[definition.id] = {
      id: definition.id,
      label: definition.label,
      description: definition.description,
      enabled,
      parameters,
    };
    return acc;
  }, {});
};

const getUniverseRuleDefinitions = () => {
  return Object.values(RULE_DEFINITIONS).map((definition) => ({
    ...definition,
    parameters: { ...(definition.parameters || {}) },
  }));
};

const normalizeDateOnly = (value) => {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()));
  }
  const text = String(value).slice(0, 10);
  const parsed = new Date(`${text}T00:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toDateKey = (value) => {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const text = String(value).slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
};

const shiftDateKey = (dateKey, deltaDays) => {
  const parsed = normalizeDateOnly(dateKey);
  if (!parsed) return null;
  parsed.setUTCDate(parsed.getUTCDate() + Number(deltaDays || 0));
  return parsed.toISOString().slice(0, 10);
};

const toNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toUniverseDateKey = (value) => {
  const normalized = normalizeAsOfDate(value) || new Date().toISOString().slice(0, 10);
  return String(normalized).slice(0, 10).replace(/-/g, "");
};

const stableStringifyRuleConfig = (ruleConfig = {}) => {
  return Object.keys(ruleConfig)
    .sort()
    .map((ruleId) => {
      const rule = ruleConfig[ruleId] || {};
      const enabled = Boolean(rule.enabled);
      const parameters = Object.keys(rule.parameters || {})
        .sort()
        .map((key) => `${key}=${rule.parameters?.[key]}`);
      return `${ruleId}:${enabled ? 1 : 0}:${parameters.join(",")}`;
    })
    .join("|");
};

const hashUniverseSignature = (input = "") => {
  let hash = 2166136261;
  const text = String(input || "");
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0").slice(0, 8);
};

const buildUniverseStateKey = ({ asOfDate, ruleConfig = {} } = {}) => {
  const dateKey = toUniverseDateKey(asOfDate);
  const signature = stableStringifyRuleConfig(ruleConfig);
  const rulesHash = hashUniverseSignature(signature);
  return `${UNIVERSE_STATE_VERSION}-${dateKey}-${rulesHash}`;
};

const buildCandidateUniverse = async () => {
  return stockMasterService.getEligibleEodSearchMasters();
};

const groupCandlesByMasterId = (rows = []) => {
  return rows.reduce((acc, row) => {
    const masterId = Number(row?.master_id);
    if (!Number.isFinite(masterId)) return acc;
    if (!acc.has(masterId)) acc.set(masterId, []);
    acc.get(masterId).push(row);
    return acc;
  }, new Map());
};

const buildInsufficientWindowResult = (ruleId, requestedDays, availableDays) => ({
  ruleId,
  passed: false,
  reason: `Only ${availableDays} candles were available in the recent window; required at least ${requestedDays}.`,
  meta: {
    skipped: false,
    insufficientWindow: true,
    requestedDays,
    availableDays,
  },
});

const evaluateRuleResults = ({ candles, asOfDate, ruleConfig }) => {
  const latestCandle = candles[candles.length - 1] || null;
  const results = {};
  const failedRuleIds = [];

  const addResult = (ruleId, passed, reason = null, meta = {}) => {
    const config = ruleConfig[ruleId];
    results[ruleId] = {
      enabled: config.enabled,
      passed,
      reason: passed ? null : reason,
      parameters: { ...(config.parameters || {}) },
      meta,
    };
    if (!passed) failedRuleIds.push(ruleId);
  };

  Object.keys(ruleConfig).forEach((ruleId) => {
    const config = ruleConfig[ruleId];
    if (!config.enabled) {
      results[ruleId] = {
        enabled: false,
        passed: true,
        reason: null,
        parameters: { ...(config.parameters || {}) },
        meta: { skipped: true },
      };
      return;
    }

    if (ruleId === "has_min_history") {
      const minCandles = config.parameters.minCandles;
      const passed = candles.length >= minCandles;
      addResult(
        ruleId,
        passed,
        `Only ${candles.length} candles were available before ${asOfDate}; required at least ${minCandles}.`,
        { availableCandles: candles.length },
      );
      return;
    }

    if (ruleId === "recent_data_available") {
      const recentEntries = config.parameters.recentEntries;
      const maxGapDays = config.parameters.maxGapDays;
      const orderedCandles = candles
        .slice()
        .sort((left, right) => {
          const leftDate = normalizeDateOnly(left?.trade_date);
          const rightDate = normalizeDateOnly(right?.trade_date);
          const leftTime = leftDate ? leftDate.getTime() : 0;
          const rightTime = rightDate ? rightDate.getTime() : 0;
          return leftTime - rightTime;
        });
      const window = orderedCandles.slice(-recentEntries);
      if (window.length < recentEntries) {
        const skipped = buildInsufficientWindowResult(ruleId, recentEntries, window.length);
        addResult(skipped.ruleId, skipped.passed, skipped.reason, skipped.meta);
        return;
      }

      const firstTradeDateKey = toDateKey(window[0]?.trade_date);
      const lastTradeDateKey = toDateKey(window[window.length - 1]?.trade_date);
      const firstTradeDate = normalizeDateOnly(firstTradeDateKey);
      const lastTradeDate = normalizeDateOnly(lastTradeDateKey);
      const gapDays = firstTradeDate && lastTradeDate
        ? Math.round((lastTradeDate.getTime() - firstTradeDate.getTime()) / (24 * 60 * 60 * 1000))
        : null;
      const passed = gapDays !== null ? gapDays < maxGapDays : false;
      addResult(
        ruleId,
        passed,
        gapDays === null
          ? `Could not compute the calendar span for the last ${recentEntries} trade entries.`
          : `The last ${recentEntries} trade entries span ${gapDays} days; allowed maximum is ${maxGapDays - 1} days.`,
        {
          recentEntries,
          maxGapDays,
          checkedTradeDates: window.map((candle) => String(candle.trade_date || "").slice(0, 10)),
          firstTradeDate: firstTradeDateKey,
          lastTradeDate: lastTradeDateKey,
          gapDays,
          latestTradeDate: latestCandle ? String(latestCandle.trade_date || "").slice(0, 10) : null,
        },
      );
      return;
    }

    if (ruleId === "recent_trades_in_window") {
      const lookbackDays = config.parameters.lookbackDays;
      const minTrades = config.parameters.minTrades;
      const endDateKey = toDateKey(asOfDate);
      if (!endDateKey) {
        addResult(ruleId, false, "No valid as-of date was available to evaluate the recent trade window.", {
          lookbackDays,
          minTrades,
        });
        return;
      }

      const startDateKey = shiftDateKey(endDateKey, -Math.max(0, lookbackDays - 1));
      const window = candles.filter((candle) => {
        const tradeDateKey = toDateKey(candle.trade_date);
        return Boolean(tradeDateKey && startDateKey && tradeDateKey >= startDateKey && tradeDateKey <= endDateKey);
      });
      const passed = window.length >= minTrades;

      addResult(
        ruleId,
        passed,
        passed
          ? null
          : `Only ${window.length} trades were found in the last ${lookbackDays} days ending on the as-of date; required at least ${minTrades}.`,
        {
          lookbackDays,
          minTrades,
          checkedTradeDates: window.map((candle) => String(candle.trade_date || "").slice(0, 10)),
          tradeCountInWindow: window.length,
          windowStart: startDateKey,
          windowEnd: endDateKey,
          latestTradeDate: latestCandle ? String(latestCandle.trade_date || "").slice(0, 10) : null,
        },
      );
      return;
    }

    if (ruleId === "zero_volume_last_5d") {
      const days = config.parameters.days;
      const window = candles.slice(-days);
      if (window.length < days) {
        const skipped = buildInsufficientWindowResult(ruleId, days, window.length);
        addResult(skipped.ruleId, skipped.passed, skipped.reason, skipped.meta);
        return;
      }

      const zeroVolumeDays = window.filter((candle) => toNumber(candle.volume, 0) <= 0);
      const passed = zeroVolumeDays.length === 0;
      addResult(
        ruleId,
        passed,
        `${zeroVolumeDays.length} of the last ${days} candles had zero volume.`,
        {
          checkedCandles: window.length,
          zeroVolumeDays: zeroVolumeDays.length,
          zeroVolumeTradeDates: zeroVolumeDays.map((candle) => String(candle.trade_date || "").slice(0, 10)),
        },
      );
      return;
    }

  });

  return {
    passed: failedRuleIds.length === 0,
    failedRuleIds,
    results,
    latestCandle,
  };
};

const buildEligibleUniverse = async ({ asOfDate, rules = {} } = {}) => {
  const normalizedAsOfDate = normalizeAsOfDate(asOfDate) || new Date().toISOString().slice(0, 10);
  const ruleConfig = buildRuleConfig(rules);
  const universeStateKey = buildUniverseStateKey({ asOfDate: normalizedAsOfDate, ruleConfig });
  const requiredHistory = Math.max(
    1,
    ...Object.values(ruleConfig)
      .filter((rule) => rule.enabled)
      .flatMap((rule) => Object.values(rule.parameters || {})),
  );

  const candidates = await buildCandidateUniverse();
  const masterIds = candidates.map((stock) => Number(stock.id)).filter(Number.isFinite);
  const recentCandles = await eodRepo.listRecentCandlesByMasterIds(masterIds, {
    limitPerMaster: requiredHistory,
    asOfDate: normalizedAsOfDate,
  });
  const candlesByMasterId = groupCandlesByMasterId(recentCandles);

  const evaluations = candidates.map((stock) => {
    const masterId = Number(stock.id);
    const candles = candlesByMasterId.get(masterId) || [];
    const evaluation = evaluateRuleResults({
      candles,
      asOfDate: normalizedAsOfDate,
      ruleConfig,
    });

    return {
      master_id: masterId,
      symbol: stock.symbol,
      name: stock.name,
      exchange: stock.exchange,
      passed: evaluation.passed,
      failed_rule_ids: evaluation.failedRuleIds,
      failed_rule_labels: evaluation.failedRuleIds.map((ruleId) => ruleConfig[ruleId]?.label || ruleId),
      latest_trade_date: evaluation.latestCandle?.trade_date || null,
      candle_count_considered: candles.length,
      rule_results: evaluation.results,
    };
  });

  const includedStocks = evaluations.filter((item) => item.passed);
  const excludedStocks = evaluations.filter((item) => !item.passed);
  const failureCounts = excludedStocks.reduce((acc, item) => {
    item.failed_rule_ids.forEach((ruleId) => {
      acc[ruleId] = (acc[ruleId] || 0) + 1;
    });
    return acc;
  }, {});

  await stockMasterService.bulkUpdateHistoricalUniverseState(
    evaluations.map((item) => ({
      master_id: item.master_id,
      historical_universe_passed: item.passed,
    })),
  );

  return {
    as_of_date: normalizedAsOfDate,
    universe_state_key: universeStateKey,
    total_candidates: candidates.length,
    included_count: includedStocks.length,
    excluded_count: excludedStocks.length,
    applied_rules: ruleConfig,
    failure_counts: failureCounts,
    included_stocks: includedStocks,
    excluded_stocks: excludedStocks,
  };
};

const getHistoricalUniverseFilterCache = async ({ asOfDate, rules = {} } = {}) => {
  const cached = await historicalUniverseCacheRepo.getHistoricalUniverseCache({
    cacheType: "filter",
    asOfDate,
    rules,
  });
  if (!cached?.payload) return null;
  return {
    cache_key: cached.cache_key,
    cache_type: cached.cache_type,
    as_of_date: cached.as_of_date,
    rules_hash: cached.rules_hash,
    updated_at: cached.updated_at,
    payload: cached.payload,
  };
};

const saveHistoricalUniverseFilterCache = async ({ asOfDate, rules = {}, payload } = {}) => {
  if (!payload) return null;
  const keyInfo = historicalUniverseCacheRepo.buildHistoricalUniverseCacheKey({
    cacheType: "filter",
    asOfDate,
    rules,
  });
  const cached = await historicalUniverseCacheRepo.upsertHistoricalUniverseCache({
    cacheKey: keyInfo.cacheKey,
    cacheType: "filter",
    asOfDate: keyInfo.asOfDate,
    rulesHash: keyInfo.rulesHash,
    queryHash: null,
    engine: null,
    payload,
  });
  if (!cached) return null;
  return {
    cache_key: cached.cache_key,
    cache_type: cached.cache_type,
    as_of_date: cached.as_of_date,
    rules_hash: cached.rules_hash,
    updated_at: cached.updated_at,
    payload: cached.payload,
  };
};

const buildAndCacheEligibleUniverse = async ({ asOfDate, rules = {} } = {}) => {
  const payload = await buildEligibleUniverse({ asOfDate, rules });
  return {
    payload,
    cache: null,
  };
};

const searchEligibleUniverse = async ({ asOfDate, rules = {}, query = "", limit = 50, masterIds = null, universeSummary = null } = {}) => {
  const searchStartedAt = Date.now();
  const providedMasterIds = Array.isArray(masterIds)
    ? masterIds.map((item) => Number(item)).filter((value) => Number.isFinite(value) && value > 0)
    : null;

  const universeStartedAt = Date.now();
  const universe = providedMasterIds
    ? {
        as_of_date: normalizeAsOfDate(asOfDate) || new Date().toISOString().slice(0, 10),
        total_candidates: Number(universeSummary?.total_candidates || providedMasterIds.length),
        included_count: Number(universeSummary?.included_count || providedMasterIds.length),
        excluded_count: Number(universeSummary?.excluded_count || 0),
        failure_counts: universeSummary?.failure_counts || {},
        applied_rules: universeSummary?.applied_rules || buildRuleConfig(rules),
      }
    : await buildEligibleUniverse({ asOfDate, rules });
  const universeDurationMs = Date.now() - universeStartedAt;

  const includedMasterIds = providedMasterIds || (universe.included_stocks || [])
    .map((item) => Number(item.master_id))
    .filter((value) => Number.isFinite(value) && value > 0);

  const queryStartedAt = Date.now();
  const searchResult = await stockSearchService.searchStocks({
    query,
    limit,
    asOfDate: universe.as_of_date,
    masterIds: includedMasterIds,
  });
  const queryDurationMs = Date.now() - queryStartedAt;
  const totalDurationMs = Date.now() - searchStartedAt;

      return {
      ...searchResult,
      universe: {
        as_of_date: universe.as_of_date,
        universe_state_key: universe.universe_state_key || null,
        total_candidates: universe.total_candidates,
        included_count: universe.included_count,
        excluded_count: universe.excluded_count,
        failure_counts: universe.failure_counts,
        applied_rules: universe.applied_rules,
    },
    universe_details: universe,
    timings: {
      universe_duration_ms: universeDurationMs,
      query_duration_ms: queryDurationMs,
      total_duration_ms: totalDurationMs,
      included_stock_count: includedMasterIds.length,
      used_provided_master_ids: Boolean(providedMasterIds),
    },
  };
};

const searchEligibleUniverseUsingSplitData = async ({ asOfDate, rules = {}, query = "", limit = 50, masterIds = null, universeSummary = null } = {}) => {
  const searchStartedAt = Date.now();
  const providedMasterIds = Array.isArray(masterIds)
    ? masterIds.map((item) => Number(item)).filter((value) => Number.isFinite(value) && value > 0)
    : null;
  const normalizedAsOfDate = normalizeAsOfDate(asOfDate) || new Date().toISOString().slice(0, 10);
  const stateMasters = !providedMasterIds
    ? await stockMasterService.getEligibleHistoricalUniversePassedMasters()
    : null;
  const stateMasterIds = Array.isArray(stateMasters)
    ? stateMasters.map((item) => Number(item.id)).filter((value) => Number.isFinite(value) && value > 0)
    : null;

  const universeStartedAt = Date.now();
  const universe = providedMasterIds || stateMasterIds
    ? {
        as_of_date: normalizedAsOfDate,
        total_candidates: Number(universeSummary?.total_candidates || (providedMasterIds || stateMasterIds || []).length),
        included_count: Number(universeSummary?.included_count || (providedMasterIds || stateMasterIds || []).length),
        excluded_count: Number(universeSummary?.excluded_count || 0),
        failure_counts: universeSummary?.failure_counts || {},
        applied_rules: universeSummary?.applied_rules || buildRuleConfig(rules),
      }
    : await buildEligibleUniverse({ asOfDate, rules });
  const universeDurationMs = Date.now() - universeStartedAt;

  const includedMasterIds = providedMasterIds || stateMasterIds || (universe.included_stocks || [])
    .map((item) => Number(item.master_id))
    .filter((value) => Number.isFinite(value) && value > 0);

  const queryStartedAt = Date.now();
  const searchResult = await stockSearchService.searchStocksUsingSplitData({
    query,
    limit,
    asOfDate: universe.as_of_date,
    masterIds: includedMasterIds,
  });
  const queryDurationMs = Date.now() - queryStartedAt;
  const totalDurationMs = Date.now() - searchStartedAt;

  return {
    ...searchResult,
    universe: {
      as_of_date: universe.as_of_date,
      universe_state_key: universe.universe_state_key || null,
      total_candidates: universe.total_candidates,
      included_count: universe.included_count,
      excluded_count: universe.excluded_count,
      failure_counts: universe.failure_counts,
      applied_rules: universe.applied_rules,
    },
    universe_details: universe,
    timings: {
      universe_duration_ms: universeDurationMs,
      query_duration_ms: queryDurationMs,
      total_duration_ms: totalDurationMs,
      included_stock_count: includedMasterIds.length,
      used_provided_master_ids: Boolean(providedMasterIds),
      used_historical_universe_passed: Boolean(!providedMasterIds && stateMasterIds),
    },
    engine: "split_fundamentals_plus_eod",
  };
};

const searchEligibleUniverseUsingSplitDataFast = async ({ asOfDate, rules = {}, query = "", limit = 50, masterIds = null, universeSummary = null } = {}) => {
  const searchStartedAt = Date.now();
  const providedMasterIds = Array.isArray(masterIds)
    ? masterIds.map((item) => Number(item)).filter((value) => Number.isFinite(value) && value > 0)
    : null;

  const universeStartedAt = Date.now();
  const universe = providedMasterIds
    ? {
        as_of_date: normalizeAsOfDate(asOfDate) || new Date().toISOString().slice(0, 10),
        total_candidates: Number(universeSummary?.total_candidates || providedMasterIds.length),
        included_count: Number(universeSummary?.included_count || providedMasterIds.length),
        excluded_count: Number(universeSummary?.excluded_count || 0),
        failure_counts: universeSummary?.failure_counts || {},
        applied_rules: null,
      }
    : {
        as_of_date: normalizeAsOfDate(asOfDate) || new Date().toISOString().slice(0, 10),
        total_candidates: 0,
        included_count: 0,
        excluded_count: 0,
        failure_counts: {},
        applied_rules: null,
      };
  const universeDurationMs = Date.now() - universeStartedAt;

  const includedMasterIds = providedMasterIds || null;

  const queryStartedAt = Date.now();
  const searchResult = await stockSearchService.searchStocksUsingSplitDataFast({
    query,
    limit,
    asOfDate: universe.as_of_date,
    masterIds: includedMasterIds,
  });
  const queryDurationMs = Date.now() - queryStartedAt;
  const totalDurationMs = Date.now() - searchStartedAt;

  return {
    ...searchResult,
    universe: {
      as_of_date: universe.as_of_date,
      universe_state_key: universe.universe_state_key || null,
      total_candidates: universe.total_candidates,
      included_count: searchResult.total || 0,
      excluded_count: 0,
      failure_counts: {},
      applied_rules: null,
    },
    universe_details: null,
    timings: {
      universe_duration_ms: universeDurationMs,
      query_duration_ms: queryDurationMs,
      total_duration_ms: totalDurationMs,
      included_stock_count: searchResult.total || 0,
      used_provided_master_ids: Boolean(providedMasterIds),
    },
    engine: "fast_snapshot_eod_split",
  };
};

module.exports = {
  getUniverseRuleDefinitions,
  buildEligibleUniverse,
  getHistoricalUniverseFilterCache,
  saveHistoricalUniverseFilterCache,
  buildAndCacheEligibleUniverse,
  searchEligibleUniverse,
  searchEligibleUniverseUsingSplitData,
  searchEligibleUniverseUsingSplitDataFast,
};
