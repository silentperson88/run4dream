const { normalizeAsOfDate } = require("../utils/asOfDate.utils");
const stockMasterService = require("./stockMaster.service");
const eodRepo = require("../repositories/eod.repository");
const stockSearchService = require("./stockSearch.service");

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
    label: "Recent trade gap",
    description: "Keep stocks whose recent trade dates do not have unusually large gaps between them.",
    defaultEnabled: true,
    parameters: {
      recentEntries: 4,
      maxGapDays: 5,
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
  const text = String(value).slice(0, 10);
  const parsed = new Date(`${text}T00:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const toNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const buildCandidateUniverse = async () => {
  const masters = await stockMasterService.getAllMasterStocks();
  return masters.filter((stock) => {
    const screenerStatus = String(stock?.screener_status || "").toUpperCase();
    const angeloneStatus = String(stock?.angelone_fetch_status || "").toLowerCase();
    const eodHistoryStatus = String(stock?.eod_history_status || "").toUpperCase();

    return (
      stock?.is_active === true &&
      screenerStatus === "VALID" &&
      angeloneStatus === "fetched" &&
      eodHistoryStatus !== "NO_EOD_DATA"
    );
  });
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
  passed: true,
  reason: null,
  meta: {
    skipped: true,
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
      const window = candles.slice(-recentEntries);
      if (window.length < recentEntries) {
        const skipped = buildInsufficientWindowResult(ruleId, recentEntries, window.length);
        addResult(skipped.ruleId, skipped.passed, skipped.reason, skipped.meta);
        return;
      }

      const tradeDates = window
        .map((candle) => normalizeDateOnly(candle.trade_date))
        .filter(Boolean);
      const gaps = [];
      for (let index = 1; index < tradeDates.length; index += 1) {
        const diffMs = tradeDates[index].getTime() - tradeDates[index - 1].getTime();
        gaps.push(Math.round(diffMs / (24 * 60 * 60 * 1000)));
      }
      const maxObservedGap = gaps.length ? Math.max(...gaps) : 0;
      const passed = gaps.every((gap) => gap <= maxGapDays);
      addResult(
        ruleId,
        passed,
        `Recent trade gaps reached ${maxObservedGap} days; allowed maximum is ${maxGapDays} days across the last ${recentEntries} entries.`,
        {
          recentEntries,
          maxGapDays,
          checkedTradeDates: window.map((candle) => String(candle.trade_date || "").slice(0, 10)),
          observedGaps: gaps,
          maxObservedGap,
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

  return {
    as_of_date: normalizedAsOfDate,
    total_candidates: candidates.length,
    included_count: includedStocks.length,
    excluded_count: excludedStocks.length,
    applied_rules: ruleConfig,
    failure_counts: failureCounts,
    included_stocks: includedStocks,
    excluded_stocks: excludedStocks,
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
      total_candidates: universe.total_candidates,
      included_count: universe.included_count,
      excluded_count: universe.excluded_count,
      failure_counts: universe.failure_counts,
      applied_rules: universe.applied_rules,
    },
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
      total_candidates: universe.total_candidates,
      included_count: universe.included_count,
      excluded_count: universe.excluded_count,
      failure_counts: universe.failure_counts,
      applied_rules: universe.applied_rules,
    },
    timings: {
      universe_duration_ms: universeDurationMs,
      query_duration_ms: queryDurationMs,
      total_duration_ms: totalDurationMs,
      included_stock_count: includedMasterIds.length,
      used_provided_master_ids: Boolean(providedMasterIds),
    },
    engine: "split_fundamentals_plus_eod",
  };
};

module.exports = {
  getUniverseRuleDefinitions,
  buildEligibleUniverse,
  searchEligibleUniverse,
  searchEligibleUniverseUsingSplitData,
};
