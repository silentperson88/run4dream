const { pool } = require("../config/db");
const { buildValueAnalysisRows } = require("./valueAnalysis.service");
const stockMasterService = require("./stockMaster.service");
const activeStocksRepo = require("../repositories/activeStocks.repository");
const eodRepo = require("../repositories/eod.repository");
const { buildPeriodRows } = require("../repositories/fundamentalsSplit.repository");
const { filterRowsByAsOfDate, asOfDateToPeriodNumericValue } = require("../utils/asOfDate.utils");

const NUMBER_OPERATORS = [">=", "<=", "!=", "==", "=", ">", "<"];
const TEXT_OPERATORS = ["contains", "starts with", "ends with", "=", "!="];
const BOOLEAN_OPERATORS = ["=", "!="];
const FIELD_PATTERN = /^(.+?)\s*(>=|<=|!=|==|=|>|<|contains|starts with|ends with)\s*(.+)$/i;
const HISTORY_REQUIRED_FIELDS = new Set([
  "sales_growth_1y",
  "sales_growth_3y",
  "sales_growth_5y",
  "profit_growth_1y",
  "profit_growth_3y",
  "profit_growth_5y",
  "eps_growth_1y",
  "eps_growth_3y",
  "eps_growth_5y",
  "average_roe_3y",
  "average_roe_5y",
  "average_roce_3y",
  "average_roce_5y",
  "promoter_holding_change_1q",
  "promoter_holding_change_4q",
  "promoter_max_quarter_drop",
  "promoter_trend",
  "fii_holding_change_1q",
  "fii_holding_change_4q",
  "fii_trend",
  "dii_holding_change_1q",
  "dii_holding_change_4q",
  "dii_trend",
  "public_holding_change_1q",
  "public_holding_change_4q",
  "public_trend",
  "dividend_paying_last_3_years",
  "margin_stability",
  "roe_stability",
  "roce_stability",
  "opm_change",
  "interest_coverage_change",
  "debtor_days_change",
  "inventory_days_change",
  "working_capital_days_change",
  "profit_positive_last_3_years",
  "operating_cash_flow_positive_last_3_years",
  "sales_growth_consistency",
  "profit_growth_consistency",
]);

const SLOW_SEARCH_FIELDS = new Set([
  "sales_growth_1y",
  "sales_growth_3y",
  "sales_growth_5y",
  "profit_growth_1y",
  "profit_growth_3y",
  "profit_growth_5y",
  "eps_growth_1y",
  "eps_growth_3y",
  "eps_growth_5y",
  "average_roe_3y",
  "average_roe_5y",
  "average_roce_3y",
  "average_roce_5y",
  "promoter_holding_change_1q",
  "promoter_holding_change_4q",
  "promoter_max_quarter_drop",
  "promoter_trend",
  "fii_holding_change_1q",
  "fii_holding_change_4q",
  "fii_trend",
  "dii_holding_change_1q",
  "dii_holding_change_4q",
  "dii_trend",
  "public_holding_change_1q",
  "public_holding_change_4q",
  "public_trend",
  "dividend_paying_last_3_years",
  "margin_stability",
  "roe_stability",
  "roce_stability",
  "opm_change",
  "interest_coverage_change",
  "debtor_days_change",
  "inventory_days_change",
  "working_capital_days_change",
  "profit_positive_last_3_years",
  "operating_cash_flow_positive_last_3_years",
  "sales_growth_consistency",
  "profit_growth_consistency",
]);

const EOD_HISTORY_LIMIT_BY_FIELD = {
  return_1w: 6,
  return_1m: 1,
  return_3m: 1,
  return_6m: 1,
  return_1y: 1,
  dma_20: 1,
  dma_50: 1,
  dma_200: 1,
  price_vs_dma_50_percent: 1,
  price_vs_dma_200_percent: 1,
  dma_50_vs_dma_200: 1,
  price_from_52_week_high_percent: 1,
  price_from_52_week_low_percent: 1,
  week_52_high: 1,
  week_52_low: 1,
  week_52_high_breakout: 1,
  all_time_high: 1,
  all_time_high_breakout: 1,
};

const SPLIT_TABLE_BY_FIELD = new Map([
  ["sales_growth_1y", "profit_loss"],
  ["sales_growth_3y", "profit_loss"],
  ["sales_growth_5y", "profit_loss"],
  ["profit_growth_1y", "profit_loss"],
  ["profit_growth_3y", "profit_loss"],
  ["profit_growth_5y", "profit_loss"],
  ["eps_growth_1y", "profit_loss"],
  ["eps_growth_3y", "profit_loss"],
  ["eps_growth_5y", "profit_loss"],
  ["average_roe_3y", "ratios"],
  ["average_roe_5y", "ratios"],
  ["average_roce_3y", "ratios"],
  ["average_roce_5y", "ratios"],
  ["roe", "ratios"],
  ["roce", "ratios"],
  ["debt_to_equity", "ratios"],
  ["interest_coverage", "ratios"],
  ["interest_coverage_change", "ratios"],
  ["debtor_days", "ratios"],
  ["debtor_days_change", "ratios"],
  ["inventory_days", "ratios"],
  ["inventory_days_change", "ratios"],
  ["days_payable", "ratios"],
  ["working_capital_days", "ratios"],
  ["working_capital_days_change", "ratios"],
  ["cash_conversion_cycle", "ratios"],
  ["promoter_holding", "shareholding"],
  ["promoter_holding_change_1q", "shareholding"],
  ["promoter_holding_change_4q", "shareholding"],
  ["promoter_max_quarter_drop", "shareholding"],
  ["promoter_trend", "shareholding"],
  ["fii_holding", "shareholding"],
  ["fii_holding_change_1q", "shareholding"],
  ["fii_holding_change_4q", "shareholding"],
  ["fii_trend", "shareholding"],
  ["dii_holding", "shareholding"],
  ["dii_holding_change_1q", "shareholding"],
  ["dii_holding_change_4q", "shareholding"],
  ["dii_trend", "shareholding"],
  ["public_holding", "shareholding"],
  ["public_holding_change_1q", "shareholding"],
  ["public_holding_change_4q", "shareholding"],
  ["public_trend", "shareholding"],
  ["dividend_yield", "profit_loss"],
  ["dividend_payout_ratio", "profit_loss"],
  ["operating_profit_margin", "profit_loss"],
  ["opm_change", "profit_loss"],
  ["profit_positive_last_3_years", "profit_loss"],
  ["operating_cash_flow_positive_last_3_years", "cash_flow"],
  ["dividend_paying_last_3_years", "profit_loss"],
  ["margin_stability", "profit_loss"],
  ["roe_stability", "ratios"],
  ["roce_stability", "ratios"],
  ["sales", "profit_loss"],
  ["revenue", "profit_loss"],
  ["operating_profit", "profit_loss"],
  ["net_profit", "profit_loss"],
  ["eps", "profit_loss"],
  ["borrowings", "balance_sheet"],
  ["reserves", "balance_sheet"],
  ["equity_capital", "balance_sheet"],
  ["cash_equivalents", "balance_sheet"],
  ["total_liabilities", "balance_sheet"],
  ["total_assets", "balance_sheet"],
  ["net_worth", "balance_sheet"],
  ["cash_from_operating_activity", "cash_flow"],
  ["net_cash_flow", "cash_flow"],
  ["book_value", "balance_sheet"],
  ["face_value", "balance_sheet"],
  ["number_of_shareholders", "shareholding"],
  ["ltp", "active"],
  ["lower_circuit", "active"],
  ["upper_circuit", "active"],
]);

const EOD_FIELD_KEYS = new Set([
  "eod_close",
  "eod_open",
  "eod_high",
  "eod_low",
  "eod_volume",
  "eod_average_price",
  "return_1d",
  "return_1w",
  "return_1m",
  "return_3m",
  "return_6m",
  "return_1y",
  "dma_10",
  "dma_20",
  "dma_50",
  "dma_100",
  "dma_200",
  "dma_10_vs_dma_20",
  "dma_10_vs_dma_50",
  "dma_10_vs_dma_100",
  "dma_10_vs_dma_200",
  "dma_20_vs_dma_50",
  "dma_20_vs_dma_100",
  "dma_20_vs_dma_200",
  "dma_50_vs_dma_100",
  "dma_50_vs_dma_200",
  "dma_100_vs_dma_200",
  "price_vs_dma_10_percent",
  "price_vs_dma_20_percent",
  "price_vs_dma_50_percent",
  "price_vs_dma_100_percent",
  "price_vs_dma_200_percent",
  "average_volume_20d",
  "avg_volume_20d",
  "average_volume_50d",
  "avg_traded_value_20d",
  "traded_days_20d",
  "is_liquid",
  "volume_spike_20d",
  "volume_ratio",
  "volatility_20d",
  "eod_52_week_high",
  "eod_52_week_low",
  "week_52_high",
  "week_52_low",
  "distance_from_52_week_high_percent",
  "distance_from_52_week_low_percent",
  "distance_from_52w_high_pct",
  "distance_from_52w_low_pct",
  "week_52_high_breakout",
  "all_time_high",
  "distance_from_ath_pct",
  "all_time_high_breakout",
  "atr_14",
  "atr_pct",
  "rsi_14",
  "macd_line",
  "macd_signal",
  "adx_14",
  "supertrend_signal",
  "close_above_10_dma",
  "close_above_20_dma",
  "close_above_50_dma",
  "close_above_200_dma",
]);

const ACTIVE_FIELD_KEYS = new Set(["ltp", "lower_circuit", "upper_circuit"]);

const FAST_EOD_FIELD_KEYS = new Set([
  "eod_close",
  "eod_open",
  "eod_high",
  "eod_low",
  "eod_volume",
  "return_1w",
  "return_1m",
  "return_3m",
  "return_6m",
  "return_1y",
  "dma_20",
  "dma_50",
  "dma_200",
  "dma_50_vs_dma_200",
  "price_vs_dma_50_percent",
  "price_vs_dma_200_percent",
  "close_above_50_dma",
  "close_above_200_dma",
  "avg_volume_20d",
  "avg_traded_value_20d",
  "traded_days_20d",
  "volume_spike_20d",
  "volatility_20d",
  "week_52_high",
  "week_52_low",
  "week_52_high_breakout",
  "all_time_high",
  "all_time_high_breakout",
  "atr_14",
  "rsi_14",
  "macd_line",
  "macd_signal",
  "adx_14",
  "supertrend_signal",
  "is_liquid",
  "percent_change",
  "distance_from_52_week_high_percent",
  "distance_from_52_week_low_percent",
]);

const FAST_SPLIT_FIELD_KEYS = new Set([
  "promoter_holding",
  "fii_holding",
  "dii_holding",
  "roe",
  "roce",
  "opm",
  "debt_to_equity",
  "interest_coverage",
  "debtor_days",
  "working_capital_days",
  "sales",
  "net_profit",
  "eps",
]);

const FAST_ACTIVE_FIELD_KEYS = new Set(["lower_circuit", "upper_circuit"]);

const FAST_QUERY_FIELD_KEYS = new Set([
  ...FAST_EOD_FIELD_KEYS,
  ...FAST_SPLIT_FIELD_KEYS,
  ...FAST_ACTIVE_FIELD_KEYS,
  "symbol",
  "company_name",
  "return_1d",
]);

const FAST_QUERY_HISTORY_ONLY_KEYS = new Set([
  "sales_growth_1y",
  "sales_growth_3y",
  "sales_growth_5y",
  "profit_growth_1y",
  "profit_growth_3y",
  "profit_growth_5y",
  "eps_growth_1y",
  "eps_growth_3y",
  "eps_growth_5y",
  "average_roe_3y",
  "average_roe_5y",
  "average_roce_3y",
  "average_roce_5y",
  "promoter_holding_change_1q",
  "promoter_holding_change_4q",
  "promoter_max_quarter_drop",
  "promoter_trend",
  "fii_holding_change_1q",
  "fii_holding_change_4q",
  "fii_trend",
  "dii_holding_change_1q",
  "dii_holding_change_4q",
  "dii_trend",
  "public_holding_change_1q",
  "public_holding_change_4q",
  "public_trend",
  "dividend_paying_last_3_years",
  "margin_stability",
  "roe_stability",
  "roce_stability",
  "opm_change",
  "interest_coverage_change",
  "debtor_days_change",
  "inventory_days_change",
  "working_capital_days_change",
  "profit_positive_last_3_years",
  "operating_cash_flow_positive_last_3_years",
  "sales_growth_consistency",
  "profit_growth_consistency",
]);

const FAST_QUERY_SLOW_KEYS = new Set([
  "price_to_earning",
  "price_to_book",
  "ev_ebitda",
  "market_cap",
  "book_value",
  "dividend_yield",
]);

const isFastFundamentalsQuery = (preparedClauses = []) => {
  for (const clause of preparedClauses) {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    for (const key of keys) {
      if (!FAST_QUERY_FIELD_KEYS.has(key)) return false;
      if (FAST_QUERY_HISTORY_ONLY_KEYS.has(key)) return false;
      if (FAST_QUERY_SLOW_KEYS.has(key)) return false;
    }
  }
  return true;
};

const getQueryDataNeeds = (preparedClauses = []) => {
  const needs = {
    splitTables: new Set(),
    historicalSplitTables: new Set(),
    needsEod: false,
    eodHistoryLimit: 1,
    needsActive: false,
  };

  for (const clause of preparedClauses) {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    for (const key of keys) {
      const splitTable = SPLIT_TABLE_BY_FIELD.get(key);
      if (splitTable) needs.splitTables.add(splitTable);
      if ((HISTORY_REQUIRED_FIELDS.has(key) || SLOW_SEARCH_FIELDS.has(key)) && splitTable) {
        needs.historicalSplitTables.add(splitTable);
      }
      if (EOD_FIELD_KEYS.has(key)) {
        needs.needsEod = true;
        needs.eodHistoryLimit = Math.max(needs.eodHistoryLimit, EOD_HISTORY_LIMIT_BY_FIELD[key] || 1);
      }
      if (ACTIVE_FIELD_KEYS.has(key)) needs.needsActive = true;
    }
  }

  return needs;
};

const buildField = (config) => ({
  ...config,
  aliases: Array.isArray(config.aliases) ? config.aliases : [],
  unit: config.unit || null,
});

const normalize = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toBoolean = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  const text = String(value || "").trim().toLowerCase();
  if (!text) return null;
  if (["true", "yes", "y", "1", "pass"].includes(text)) return true;
  if (["false", "no", "n", "0", "fail"].includes(text)) return false;
  return null;
};

const average = (values = []) => {
  const valid = values.map(toNumber).filter((value) => value !== null);
  if (!valid.length) return null;
  return valid.reduce((sum, value) => sum + value, 0) / valid.length;
};

const stddev = (values = []) => {
  const avg = average(values);
  const valid = values.map(toNumber).filter((value) => value !== null);
  if (avg === null || valid.length < 2) return null;
  const variance = valid.reduce((sum, value) => sum + Math.pow(value - avg, 2), 0) / valid.length;
  return Math.sqrt(variance);
};

const normalizeOperator = (value = "") => String(value || "").trim().toLowerCase().replace(/\s+/g, " ");

const compareNumber = (value, operator, expected) => {
  const left = toNumber(value);
  const right = toNumber(expected);
  if (left === null || right === null) return false;

  switch (operator) {
    case ">":
      return left > right;
    case ">=":
      return left >= right;
    case "<":
      return left < right;
    case "<=":
      return left <= right;
    case "=":
    case "==":
      return left === right;
    case "!=":
      return left !== right;
    default:
      return false;
  }
};

const compareText = (value, operator, expected) => {
  const left = String(value || "").trim().toLowerCase();
  const right = String(expected || "").trim().toLowerCase();
  if (!left || !right) return false;

  switch (operator) {
    case "contains":
      return left.includes(right);
    case "starts with":
      return left.startsWith(right);
    case "ends with":
      return left.endsWith(right);
    case "=":
    case "==":
      return left === right;
    case "!=":
      return left !== right;
    default:
      return false;
  }
};

const compareBoolean = (value, operator, expected) => {
  const left = toBoolean(value);
  const right = toBoolean(expected);
  if (left === null || right === null) return false;
  if (operator === "!=") return left !== right;
  return left === right;
};

const compare = (field, value, operator, expected, expectedField = null, row = null) => {
  if (!field) return false;
  const resolvedExpected = expectedField && row ? expectedField.getValue(row) : expected;
  if (field.type === "text") return compareText(value, operator, resolvedExpected);
  if (field.type === "boolean") return compareBoolean(value, operator, resolvedExpected);
  return compareNumber(value, operator, resolvedExpected);
};

const formatValue = (field, value) => {
  if (value === null || value === undefined) return "?";
  if (field.type === "text") return String(value);
  if (field.type === "boolean") return toBoolean(value) ? "Yes" : "No";

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  if (field.unit === "%") return `${numeric.toFixed(2)}%`;
  if (field.unit === "years") return `${numeric.toFixed(1)} yrs`;
  if (field.unit === "days") return `${numeric.toFixed(0)} days`;
  if (field.unit === "pp") return `${numeric.toFixed(2)} pp`;
  if (field.unit === "Rs") return `Rs. ${numeric.toFixed(2)}`;
  if (field.unit === "Cr") return `${numeric.toFixed(2)} Cr`;
  if (field.unit === "x") return `${numeric.toFixed(2)}x`;
  return numeric.toFixed(2);
};

const resolveFieldCandidates = (queryText = "") => {
  const term = normalize(queryText);
  if (!term) return [];

  return SEARCH_FIELDS.map((field) => {
    const haystacks = [field.label, field.key, ...(field.aliases || [])].map((item) => normalize(item));
    let score = 0;

    haystacks.forEach((haystack) => {
      if (!haystack) return;
      if (haystack === term) {
        score = Math.max(score, 120);
        return;
      }
      if (haystack.startsWith(term)) {
        score = Math.max(score, 95);
        return;
      }
      if (haystack.includes(term)) {
        score = Math.max(score, 70);
        return;
      }

      const tokens = term.split(" ").filter(Boolean);
      const tokenScore = tokens.reduce((acc, token) => (haystack.includes(token) ? acc + 10 : acc), 0);
      score = Math.max(score, tokenScore);
    });

    return { ...field, score };
  })
    .filter((field) => field.score > 0)
    .sort((a, b) => b.score - a.score || String(a.label).localeCompare(String(b.label)));
};

const splitClauses = (query = "") =>
  String(query || "")
    .replace(/\r/g, "\n")
    .split(/\n|[,;]+/g)
    .flatMap((line) => line.split(/\s+(?:and|&&)\s+/i))
    .map((clause) =>
      clause
        .replace(/^\s*(?:and|&&)\s+/i, "")
        .replace(/\s+(?:and|&&)\s*$/i, "")
        .trim(),
    )
    .filter(Boolean);

const parseQuery = (query = "") =>
  splitClauses(query).map((clause) => {
    const match = clause.match(FIELD_PATTERN);
    if (!match) {
      return { raw: clause, fieldText: clause, operator: null, valueText: null };
    }

    return {
      raw: clause,
      fieldText: match[1].trim(),
      operator: normalizeOperator(match[2]),
      valueText: match[3].trim(),
    };
  });

const groupByMasterId = (rows = []) =>
  rows.reduce((acc, row) => {
    const key = String(row?.master_id || "");
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});

const cleanSeries = (rows = [], key) =>
  rows
    .map((row) => toNumber(typeof key === "function" ? key(row) : row?.[key]))
    .filter((value) => value !== null);

const getLatest = (rows = [], key) => {
  const values = cleanSeries(rows, key);
  return values.length ? values[values.length - 1] : null;
};

const getPrevious = (rows = [], key) => {
  const values = cleanSeries(rows, key);
  return values.length > 1 ? values[values.length - 2] : null;
};

const getGrowthPercent = (startValue, endValue) => {
  const start = toNumber(startValue);
  const end = toNumber(endValue);
  if (start === null || end === null || start <= 0) return null;
  return ((end - start) / start) * 100;
};

const getCagr = (rows = [], key, years) => {
  const values = cleanSeries(rows, key);
  if (values.length < years + 1) return null;
  const start = values[values.length - (years + 1)];
  const end = values[values.length - 1];
  if (start <= 0 || end <= 0) return null;
  return (Math.pow(end / start, 1 / years) - 1) * 100;
};

const averageLastN = (rows = [], key, count) => {
  const values = cleanSeries(rows, key);
  return average(values.slice(Math.max(0, values.length - count)));
};

const countPositiveLastN = (rows = [], key, count) => {
  const values = cleanSeries(rows, key);
  const slice = values.slice(Math.max(0, values.length - count));
  if (slice.length < count) return false;
  return slice.every((value) => value > 0);
};

const classifyNetTrend = (netChange, stableBand = 0.5) => {
  if (netChange === null || netChange === undefined) return "missing";
  if (netChange > stableBand) return "increasing";
  if (netChange < -stableBand) return "decreasing";
  return "stable";
};

const classifyPublicTrend = (netChange) => {
  if (netChange === null || netChange === undefined) return "missing";
  if (netChange < 0) return "decreasing";
  if (netChange < 2) return "stable";
  return "increasing";
};

const getSeriesTrend = (values = []) => {
  const valid = values.map(toNumber).filter((value) => value !== null);
  if (valid.length < 2 || valid.length !== values.length) {
    return {
      hasEnough: false,
      netChange: null,
      maxQuarterDrop: null,
      latestChange: null,
    };
  }

  let maxQuarterDrop = 0;
  for (let i = 1; i < valid.length; i += 1) {
    const drop = valid[i - 1] - valid[i];
    if (drop > maxQuarterDrop) maxQuarterDrop = drop;
  }

  return {
    hasEnough: true,
    netChange: valid[valid.length - 1] - valid[0],
    maxQuarterDrop,
    latestChange: valid[valid.length - 1] - valid[valid.length - 2],
  };
};

const isStableRange = (values = [], tolerance) => {
  const valid = values.map(toNumber).filter((value) => value !== null);
  if (valid.length < 3) return false;
  return Math.max(...valid) - Math.min(...valid) <= tolerance;
};

const getLatestCandleValue = (candles = [], key) => {
  const candle = candles[candles.length - 1];
  return candle ? toNumber(candle[key]) : null;
};

const getOffsetCandleValue = (candles = [], offset, key = "close") => {
  const index = candles.length - 1 - offset;
  if (index < 0) return null;
  return toNumber(candles[index]?.[key]);
};

const averageCandleMetric = (candles = [], key, count) => {
  const slice = candles.slice(Math.max(0, candles.length - count));
  return average(slice.map((item) => item?.[key]));
};

const getReturnPct = (candles = [], offset) => {
  const latest = getLatestCandleValue(candles, "close");
  const previous = getOffsetCandleValue(candles, offset, "close");
  return getGrowthPercent(previous, latest);
};

const buildEodMetrics = (candles = []) => {
  const latestSnapshot = candles[candles.length - 1] || {};
  const latestOpen = toNumber(latestSnapshot.open ?? getLatestCandleValue(candles, "open"));
  const latestHigh = toNumber(latestSnapshot.high ?? getLatestCandleValue(candles, "high"));
  const latestLow = toNumber(latestSnapshot.low ?? getLatestCandleValue(candles, "low"));
  const latestClose = toNumber(latestSnapshot.close ?? getLatestCandleValue(candles, "close"));
  const latestVolume = toNumber(latestSnapshot.volume ?? getLatestCandleValue(candles, "volume"));
  const latestStoredMetrics = {
    dma_20: toNumber(latestSnapshot.dma_20),
    dma_50: toNumber(latestSnapshot.dma_50),
    dma_200: toNumber(latestSnapshot.dma_200),
    return_1m: toNumber(latestSnapshot.return_1m),
    return_3m: toNumber(latestSnapshot.return_3m),
    return_6m: toNumber(latestSnapshot.return_6m),
    return_1y: toNumber(latestSnapshot.return_1y),
    week_52_high: toNumber(latestSnapshot.week_52_high),
    week_52_low: toNumber(latestSnapshot.week_52_low),
    week_52_high_breakout:
      typeof latestSnapshot.week_52_high_breakout === "boolean" ? latestSnapshot.week_52_high_breakout : null,
    all_time_high: toNumber(latestSnapshot.all_time_high),
    all_time_high_breakout:
      typeof latestSnapshot.all_time_high_breakout === "boolean" ? latestSnapshot.all_time_high_breakout : null,
    avg_volume_20d: toNumber(latestSnapshot.avg_volume_20d),
    avg_traded_value_20d: toNumber(latestSnapshot.avg_traded_value_20d),
    traded_days_20d: toNumber(latestSnapshot.traded_days_20d),
    volatility_20d: toNumber(latestSnapshot.volatility_20d),
    atr_14: toNumber(latestSnapshot.atr_14),
    rsi_14: toNumber(latestSnapshot.rsi_14),
    macd_line: toNumber(latestSnapshot.macd_line),
    macd_signal: toNumber(latestSnapshot.macd_signal),
    adx_14: toNumber(latestSnapshot.adx_14),
    supertrend_signal:
      latestSnapshot.supertrend_signal === null || latestSnapshot.supertrend_signal === undefined
        ? null
        : Number(latestSnapshot.supertrend_signal),
    is_liquid:
      typeof latestSnapshot.is_liquid === "boolean" ? latestSnapshot.is_liquid : null,
  };

  const dma50 = latestStoredMetrics.dma_50 ?? averageCandleMetric(candles, "close", 50);
  const dma200 = latestStoredMetrics.dma_200 ?? averageCandleMetric(candles, "close", 200);

  const avgVolume20 = latestStoredMetrics.avg_volume_20d ?? averageCandleMetric(candles, "volume", 20);
  const avgTradedValue20 = latestStoredMetrics.avg_traded_value_20d ?? null;
  const tradedDays20 = latestStoredMetrics.traded_days_20d ?? null;

  const week52High = latestStoredMetrics.week_52_high ?? null;
  const week52Low = latestStoredMetrics.week_52_low ?? null;
  const allTimeHigh = latestStoredMetrics.all_time_high ?? null;

  const return_1d = candles.length > 1 ? getReturnPct(candles, 1) : null;
  const return_1w = candles.length > 5 ? getReturnPct(candles, 5) : null;
  const return_1m = latestStoredMetrics.return_1m ?? null;
  const return_3m = latestStoredMetrics.return_3m ?? null;
  const return_6m = latestStoredMetrics.return_6m ?? null;
  const return_1y = latestStoredMetrics.return_1y ?? null;

  const priceVs = (dmaValue) => (latestClose !== null && dmaValue !== null && dmaValue !== 0 ? ((latestClose - dmaValue) / dmaValue) * 100 : null);
  const distanceFrom52wHigh =
    latestClose !== null && week52High !== null && week52High > 0 ? ((week52High - latestClose) / week52High) * 100 : null;
  const distanceFrom52wLow =
    latestClose !== null && week52Low !== null && week52Low > 0 ? ((latestClose - week52Low) / week52Low) * 100 : null;
  const distanceFromAth =
    latestClose !== null && allTimeHigh !== null && allTimeHigh > 0 ? ((allTimeHigh - latestClose) / allTimeHigh) * 100 : null;

  return {
    eod_close: latestClose,
    eod_open: latestOpen,
    eod_high: latestHigh,
    eod_low: latestLow,
    eod_volume: latestVolume,
    eod_average_price:
      [latestOpen, latestHigh, latestLow, latestClose].every((value) => value !== null)
        ? (latestOpen + latestHigh + latestLow + latestClose) / 4
        : null,
    return_1d,
    return_1w,
    return_1m,
    return_3m,
    return_6m,
    return_1y,
    dma_50: dma50,
    dma_200: dma200,
    dma_50_vs_dma_200: dma50 !== null && dma200 !== null ? dma50 - dma200 : null,
    price_vs_dma_50_percent: priceVs(dma50),
    price_vs_dma_200_percent: priceVs(dma200),
    average_volume_20d: avgVolume20,
    avg_volume_20d: avgVolume20,
    avg_traded_value_20d: avgTradedValue20,
    traded_days_20d: tradedDays20,
    is_liquid:
      latestStoredMetrics.is_liquid !== null && latestStoredMetrics.is_liquid !== undefined
        ? latestStoredMetrics.is_liquid
        : null,
    volume_spike_20d: latestVolume !== null && avgVolume20 ? latestVolume / avgVolume20 : null,
    week_52_high: week52High,
    week_52_low: week52Low,
    distance_from_52w_high_pct: distanceFrom52wHigh,
    distance_from_52w_low_pct: distanceFrom52wLow,
    week_52_high_breakout:
      latestStoredMetrics.week_52_high_breakout ??
      (latestClose !== null && week52High !== null ? latestClose >= week52High : null),
    all_time_high: allTimeHigh,
    distance_from_ath_pct: distanceFromAth,
    all_time_high_breakout:
      latestStoredMetrics.all_time_high_breakout ??
      (latestClose !== null && allTimeHigh !== null ? latestClose >= allTimeHigh : null),
    atr_14: latestStoredMetrics.atr_14 ?? null,
    volatility_20d: latestStoredMetrics.volatility_20d ?? null,
    rsi_14: latestStoredMetrics.rsi_14 ?? null,
    macd_line: latestStoredMetrics.macd_line ?? null,
    macd_signal: latestStoredMetrics.macd_signal ?? null,
    adx_14: latestStoredMetrics.adx_14 ?? null,
    supertrend_signal: latestStoredMetrics.supertrend_signal ?? null,
    close_above_50_dma: latestClose !== null && dma50 !== null ? latestClose > dma50 : null,
    close_above_200_dma: latestClose !== null && dma200 !== null ? latestClose > dma200 : null,
  };
};

const fieldValue = (key) => (row) => row?.search_metrics?.[key] ?? null;

const buildRequestedMetricKeys = (preparedClauses = []) => {
  const keys = new Set();
  for (const clause of preparedClauses) {
    const clauseKeys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    clauseKeys.forEach((key) => keys.add(key));
  }
  return keys;
};

const getFromObject = (record = {}, keys = []) => {
  for (const key of keys) {
    if (record && record[key] !== undefined && record[key] !== null && record[key] !== "") {
      return record[key];
    }
  }
  return null;
};

const getNumberFromObject = (record = {}, keys = []) => {
  const value = getFromObject(record, keys);
  return toNumber(value);
};

const buildSearchMetrics = (row = {}, activeRow = null, candles = [], options = {}) => {
  const fastMode = options.fastMode === true;
  const valueMetrics = row.value_metrics || {};
  const analysisMetrics = row.analysis?.metrics || {};
  const profitRows = Array.isArray(row.profit_loss_history) ? row.profit_loss_history : [];
  const cashRows = Array.isArray(row.cash_flow_history) ? row.cash_flow_history : [];
  const balanceRows = Array.isArray(row.balance_history) ? row.balance_history : [];
  const ratioRows = Array.isArray(row.ratio_history) ? row.ratio_history : [];
  const shareRows = Array.isArray(row.shareholding_history) ? row.shareholding_history : [];

  const latestProfit = profitRows[profitRows.length - 1] || {};
  const latestCash = cashRows[cashRows.length - 1] || {};
  const latestBalance = balanceRows[balanceRows.length - 1] || {};
  const latestRatio = ratioRows[ratioRows.length - 1] || {};
  const latestShare = shareRows[shareRows.length - 1] || {};

  const promoterStats = fastMode ? null : getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["promoters"])));
  const fiiStats = fastMode ? null : getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["fiis"])));
  const diiStats = fastMode ? null : getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["diis"])));
  const publicStats = fastMode ? null : getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["public"])));

  const latestInventoryDays = getNumberFromObject(latestRatio, ["inventory_days"]);
  const latestDebtorDays = getNumberFromObject(latestRatio, ["debtor_days", "receivable_days"]);
  const latestDaysPayable = getNumberFromObject(latestRatio, ["days_payable", "payable_days"]);
  const latestWorkingCapitalDays = getNumberFromObject(latestRatio, ["working_capital_days"]);
  const latestCashConversionCycle =
    getNumberFromObject(latestRatio, ["cash_conversion_cycle"]) ??
    (latestInventoryDays !== null && latestDebtorDays !== null && latestDaysPayable !== null
      ? latestInventoryDays + latestDebtorDays - latestDaysPayable
      : null);

  const latestRoe = valueMetrics.roe ?? analysisMetrics.roe ?? getNumberFromObject(latestRatio, ["roe_percent", "roe"]);
  const latestRoce =
    valueMetrics.roce ?? analysisMetrics.roce ?? getNumberFromObject(latestRatio, ["roce_percent", "roce"]);
  const latestDebtToEquity =
    valueMetrics.debt_to_equity ??
    analysisMetrics.debt_to_equity ??
    getNumberFromObject(latestRatio, ["debt_to_equity"]);
  const latestOpm =
    valueMetrics.opm_percent ??
    analysisMetrics.opm_percent ??
    getNumberFromObject(latestProfit, ["opm_percent", "operating_profit_margin"]);
  const latestInterestCoverage =
    valueMetrics.interest_coverage ??
    analysisMetrics.interest_coverage ??
    getNumberFromObject(latestRatio, ["interest_coverage", "interest_coverage_ratio"]);

  const bookValue = toNumber(row.book_value ?? row.bookValue);
  const faceValue = toNumber(row.face_value ?? row.faceValue);
  const marketCap = toNumber(row.market_cap ?? row.marketCap);
  const totalAssets = getNumberFromObject(latestBalance, ["total_assets"]);
  const totalLiabilities = getNumberFromObject(latestBalance, ["total_liabilities"]);
  const reserves = getNumberFromObject(latestBalance, ["reserves"]);
  const equityCapital = getNumberFromObject(latestBalance, ["equity_capital"]);
  const dividendHistory = cleanSeries(profitRows, (item) =>
    getNumberFromObject(item, ["dividend_payout_percent", "dividend_payout_ratio"]),
  );
  const netWorth =
    getNumberFromObject(latestBalance, ["net_worth"]) ??
    (totalAssets !== null && totalLiabilities !== null
      ? totalAssets - totalLiabilities
      : reserves !== null && equityCapital !== null
        ? reserves + equityCapital
        : null);

  const averageRoe3Y = fastMode ? null : averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"]), 3);
  const averageRoe5Y = fastMode ? null : averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"]), 5);
  const averageRoce3Y = fastMode ? null : averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"]), 3);
  const averageRoce5Y = fastMode ? null : averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"]), 5);

  if (requestedMetricKeys) {
    const latestStoredMetrics = candles.length ? candles[candles.length - 1] || {} : {};
    const latestVolume = toNumber(latestStoredMetrics.volume ?? latestStoredMetrics.eod_volume);
    const latestOpen = toNumber(latestStoredMetrics.open);
    const latestHigh = toNumber(latestStoredMetrics.high);
    const latestLow = toNumber(latestStoredMetrics.low);
    const latestClose = toNumber(latestStoredMetrics.close ?? latestStoredMetrics.eod_close ?? row.current_price ?? row.currentPrice ?? row.cmp ?? row.price);
    const currentPrice = latestClose;
    const week52High = toNumber(latestStoredMetrics.week_52_high ?? latestStoredMetrics.eod_52_week_high);
    const week52Low = toNumber(latestStoredMetrics.week_52_low ?? latestStoredMetrics.eod_52_week_low);
    const allTimeHigh = toNumber(latestStoredMetrics.all_time_high);
    const dma50 = toNumber(latestStoredMetrics.dma_50);
    const dma200 = toNumber(latestStoredMetrics.dma_200);
    const avgVolume20 = toNumber(latestStoredMetrics.avg_volume_20d ?? latestStoredMetrics.average_volume_20d);
    const avgTradedValue20 = toNumber(latestStoredMetrics.avg_traded_value_20d);
    const tradedDays20 = toNumber(latestStoredMetrics.traded_days_20d);
    const volatility20 = toNumber(latestStoredMetrics.volatility_20d);
    const atr14 = toNumber(latestStoredMetrics.atr_14);
    const rsi14 = toNumber(latestStoredMetrics.rsi_14);
    const macdLine = toNumber(latestStoredMetrics.macd_line);
    const macdSignal = toNumber(latestStoredMetrics.macd_signal);
    const adx14 = toNumber(latestStoredMetrics.adx_14);
    const supertrendSignal = toNumber(latestStoredMetrics.supertrend_signal);
    const return1d = toNumber(latestStoredMetrics.return_1d ?? latestStoredMetrics.percent_change) ?? (candles.length > 1 ? getReturnPct(candles, 1) : null);
    const return1w = candles.length > 5 ? getReturnPct(candles, 5) : null;
    const return1m = toNumber(latestStoredMetrics.return_1m);
    const return3m = toNumber(latestStoredMetrics.return_3m);
    const return6m = toNumber(latestStoredMetrics.return_6m);
    const return1y = toNumber(latestStoredMetrics.return_1y);
    const priceVs = (dmaValue) => (currentPrice !== null && dmaValue !== null && dmaValue !== 0 ? ((currentPrice - dmaValue) / dmaValue) * 100 : null);
    const distanceFrom52wHigh =
      currentPrice !== null && week52High !== null && week52High > 0 ? ((week52High - currentPrice) / week52High) * 100 : null;
    const distanceFrom52wLow =
      currentPrice !== null && week52Low !== null && week52Low > 0 ? ((currentPrice - week52Low) / week52Low) * 100 : null;

    const metrics = {
      symbol: row.symbol || activeRow?.symbol || null,
      company_name: row.name || row.company_name || activeRow?.name || null,
      market_cap: toNumber(row.market_cap ?? row.marketCap),
    };
    const setMetric = (key, value) => {
      if (requestedMetricKeys.has(key)) metrics[key] = value;
    };

    setMetric("current_price", currentPrice);
    setMetric("ltp", currentPrice);
    setMetric("eod_close", currentPrice);
    setMetric("eod_volume", latestVolume);
    setMetric("percent_change", return1d);
    setMetric("return_1d", return1d);
    setMetric("return_1w", return1w);
    setMetric("return_1m", return1m);
    setMetric("return_3m", return3m);
    setMetric("return_6m", return6m);
    setMetric("return_1y", return1y);
    setMetric("week_52_high", week52High);
    setMetric("week_52_low", week52Low);
    setMetric("week_52_high_breakout", latestStoredMetrics.week_52_high_breakout ?? (currentPrice !== null && week52High !== null ? currentPrice >= week52High : null));
    setMetric("distance_from_52_week_high_percent", distanceFrom52wHigh);
    setMetric("distance_from_52_week_low_percent", distanceFrom52wLow);
    setMetric("all_time_high", allTimeHigh);
    setMetric("all_time_high_breakout", latestStoredMetrics.all_time_high_breakout ?? (currentPrice !== null && allTimeHigh !== null ? currentPrice >= allTimeHigh : null));
    setMetric("avg_volume_20d", avgVolume20);
    setMetric("avg_traded_value_20d", avgTradedValue20);
    setMetric("traded_days_20d", tradedDays20);
    setMetric("volume_spike_20d", avgVolume20 && latestVolume !== null ? latestVolume / avgVolume20 : null);
    setMetric("volatility_20d", volatility20);
    setMetric("is_liquid", latestStoredMetrics.is_liquid ?? null);
    setMetric("dma_50", dma50);
    setMetric("dma_200", dma200);
    setMetric("dma_50_vs_dma_200", dma50 !== null && dma200 !== null ? dma50 - dma200 : null);
    setMetric("price_vs_dma_50_percent", priceVs(dma50));
    setMetric("price_vs_dma_200_percent", priceVs(dma200));
    setMetric("close_above_50_dma", currentPrice !== null && dma50 !== null ? currentPrice > dma50 : null);
    setMetric("close_above_200_dma", currentPrice !== null && dma200 !== null ? currentPrice > dma200 : null);
    setMetric("atr_14", atr14);
    setMetric("rsi_14", rsi14);
    setMetric("macd_line", macdLine);
    setMetric("macd_signal", macdSignal);
    setMetric("adx_14", adx14);
    setMetric("supertrend_signal", supertrendSignal);
    setMetric("price_to_earning", valueMetrics.pe_ratio ?? toNumber(row.stock_pe ?? row.pe_ratio));
    setMetric("price_to_book", currentPrice !== null && row.book_value ? currentPrice / toNumber(row.book_value) : null);
    setMetric("dividend_yield", valueMetrics.dividend_yield ?? toNumber(row.dividend_yield));
    setMetric("roe", valueMetrics.roe ?? toNumber(latestRatio.roe_percent ?? latestRatio.roe));
    setMetric("roce", valueMetrics.roce ?? toNumber(latestRatio.roce_percent ?? latestRatio.roce));
    setMetric("opm", valueMetrics.opm_percent ?? toNumber(latestProfit.opm_percent ?? latestProfit.operating_profit_margin));
    setMetric("operating_profit_margin", valueMetrics.opm_percent ?? toNumber(latestProfit.opm_percent ?? latestProfit.operating_profit_margin));
    setMetric("debt_to_equity", valueMetrics.debt_to_equity ?? toNumber(latestRatio.debt_to_equity));
    setMetric("interest_coverage", valueMetrics.interest_coverage ?? toNumber(latestRatio.interest_coverage ?? latestRatio.interest_coverage_ratio));
    setMetric("debtor_days", toNumber(latestRatio.debtor_days ?? latestRatio.receivable_days));
    setMetric("working_capital_days", toNumber(latestRatio.working_capital_days));
    setMetric("promoter_holding", valueMetrics.promoters ?? toNumber(latestShare.promoters));
    setMetric("fii_holding", valueMetrics.fiis ?? toNumber(latestShare.fiis));
    setMetric("dii_holding", valueMetrics.diis ?? toNumber(latestShare.diis));
    setMetric("sales", toNumber(latestProfit.sales ?? latestProfit.revenue));
    setMetric("net_profit", toNumber(latestProfit.net_profit ?? latestProfit.profit_after_tax ?? latestProfit.pat));
    setMetric("eps", toNumber(latestProfit.eps ?? latestProfit.net_profit_profit_for_eps));
    setMetric("book_value", toNumber(row.book_value ?? row.bookValue));
    setMetric("lower_circuit", toNumber(latestStoredMetrics.lower_circuit) ?? toNumber(activeRow?.lowerCircuit));
    setMetric("upper_circuit", toNumber(latestStoredMetrics.upper_circuit) ?? toNumber(activeRow?.upperCircuit));

    return metrics;
  }

  const eodMetrics = buildEodMetrics(candles);
  const currentPrice = eodMetrics.eod_close ?? toNumber(row.current_price ?? row.currentPrice ?? row.cmp ?? row.price);

  const metrics = {
    symbol: row.symbol || activeRow?.symbol || null,
    company_name: row.name || row.company_name || activeRow?.name || null,
    market_cap: marketCap,
    current_price: currentPrice,
    ltp: currentPrice,
    pe_ratio: analysisMetrics.pe_ratio ?? valueMetrics.pe_ratio ?? toNumber(row.stock_pe ?? row.pe_ratio),
    price_to_earning: analysisMetrics.pe_ratio ?? valueMetrics.pe_ratio ?? toNumber(row.stock_pe ?? row.pe_ratio),
    peg_ratio: toNumber(row.peg_ratio ?? analysisMetrics.peg_ratio ?? valueMetrics.peg_ratio),
    price_to_book:
      valueMetrics.price_to_book ??
      analysisMetrics.price_to_book ??
      (currentPrice !== null && bookValue ? currentPrice / bookValue : null),
    price_to_sales: valueMetrics.price_to_sales ?? analysisMetrics.price_to_sales,
    dividend_yield: valueMetrics.dividend_yield ?? analysisMetrics.dividend_yield ?? toNumber(row.dividend_yield),
    dividend_payout_ratio:
      getNumberFromObject(latestProfit, ["dividend_payout_percent", "dividend_payout_ratio"]) ??
      dividendHistory[dividendHistory.length - 1] ??
      null,
    return_on_equity: latestRoe,
    return_on_capital_employed: latestRoce,
    roe: latestRoe,
    roce: latestRoce,
    debt_to_equity: latestDebtToEquity,
    promoter_holding: valueMetrics.promoters ?? analysisMetrics.promoters ?? getNumberFromObject(latestShare, ["promoters"]),
    fii_holding: valueMetrics.fiis ?? analysisMetrics.fiis ?? getNumberFromObject(latestShare, ["fiis"]),
    dii_holding: valueMetrics.diis ?? analysisMetrics.diis ?? getNumberFromObject(latestShare, ["diis"]),
    public_holding: valueMetrics.public ?? analysisMetrics.public ?? getNumberFromObject(latestShare, ["public"]),
    operating_profit_margin: latestOpm,
    opm: latestOpm,
    interest_coverage: latestInterestCoverage,
    debtor_days: valueMetrics.debtor_days ?? analysisMetrics.debtor_days ?? latestDebtorDays,
    inventory_days: latestInventoryDays,
    days_payable: latestDaysPayable,
    working_capital_days: latestWorkingCapitalDays,
    cash_conversion_cycle: latestCashConversionCycle,
    ev_ebitda: valueMetrics.ev_ebitda ?? analysisMetrics.ev_ebitda,
    pe_vs_industry: valueMetrics.pe_vs_industry ?? analysisMetrics.pe_vs_industry,
    company_age_years: valueMetrics.company_age_years ?? analysisMetrics.company_age_years,
    sales: getNumberFromObject(latestProfit, ["sales", "revenue"]),
    revenue: getNumberFromObject(latestProfit, ["revenue", "sales"]),
    operating_profit: getNumberFromObject(latestProfit, ["operating_profit", "ebit"]),
    net_profit: getNumberFromObject(latestProfit, ["net_profit", "profit_after_tax", "pat"]),
    eps: getNumberFromObject(latestProfit, ["eps", "net_profit_profit_for_eps"]),
    borrowings: getNumberFromObject(latestBalance, ["borrowings", "borrowing"]),
    reserves,
    equity_capital: equityCapital,
    cash_equivalents: getNumberFromObject(latestBalance, ["cash_equivalents", "cash_and_cash_equivalents", "cash_bank"]),
    total_liabilities: totalLiabilities,
    total_assets: totalAssets,
    net_worth: netWorth,
    cash_from_operating_activity:
      getNumberFromObject(latestCash, ["cash_from_operating_activity", "cash_from_operating_activities", "operating_cash_flow"]) ??
      toNumber(row.cash_from_operating_activity),
    net_cash_flow: getNumberFromObject(latestCash, ["net_cash_flow"]),
    number_of_shareholders: toNumber(row.number_of_shareholders ?? row.no_of_shareholders),
    no_of_shareholders: toNumber(row.number_of_shareholders ?? row.no_of_shareholders),
    face_value: faceValue,
    book_value: bookValue,
    percent_change: eodMetrics.return_1d,
    average_price: eodMetrics.eod_average_price,
    lower_circuit: toNumber(activeRow?.lowerCircuit),
    upper_circuit: toNumber(activeRow?.upperCircuit),
    week_52_low: eodMetrics.eod_52_week_low,
    week_52_high: eodMetrics.eod_52_week_high,
  };

  metrics.price_from_52_week_high_percent =
    currentPrice !== null && metrics.week_52_high ? ((metrics.week_52_high - currentPrice) / metrics.week_52_high) * 100 : null;
  metrics.price_from_52_week_low_percent =
    currentPrice !== null && metrics.week_52_low ? ((currentPrice - metrics.week_52_low) / metrics.week_52_low) * 100 : null;

  metrics.distance_from_52_week_high_percent = eodMetrics.distance_from_52_week_high_percent;
  metrics.distance_from_52_week_low_percent = eodMetrics.distance_from_52_week_low_percent;

  if (!fastMode) {
    metrics.sales_growth_1y = getGrowthPercent(
      getPrevious(profitRows, (item) => getNumberFromObject(item, ["sales", "revenue"])),
      getLatest(profitRows, (item) => getNumberFromObject(item, ["sales", "revenue"])),
    );
  metrics.sales_growth_3y = getCagr(profitRows, (item) => getNumberFromObject(item, ["sales", "revenue"]), 3);
  metrics.sales_growth_5y = getCagr(profitRows, (item) => getNumberFromObject(item, ["sales", "revenue"]), 5);
  metrics.profit_growth_1y = getGrowthPercent(
    getPrevious(profitRows, (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"])),
    getLatest(profitRows, (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"])),
  );
    metrics.profit_growth_3y = getCagr(
      profitRows,
      (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"]),
      3,
    );
    metrics.profit_growth_5y = getCagr(
      profitRows,
      (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"]),
      5,
    );
    metrics.eps_growth_1y = getGrowthPercent(
      getPrevious(profitRows, (item) => getNumberFromObject(item, ["eps", "net_profit_profit_for_eps"])),
      getLatest(profitRows, (item) => getNumberFromObject(item, ["eps", "net_profit_profit_for_eps"])),
    );
    metrics.eps_growth_3y = getCagr(
      profitRows,
      (item) => getNumberFromObject(item, ["eps", "net_profit_profit_for_eps"]),
      3,
    );
    metrics.eps_growth_5y = getCagr(
      profitRows,
      (item) => getNumberFromObject(item, ["eps", "net_profit_profit_for_eps"]),
      5,
    );
    metrics.average_roe_3y = averageRoe3Y;
    metrics.average_roe_5y = averageRoe5Y;
    metrics.average_roce_3y = averageRoce3Y;
    metrics.average_roce_5y = averageRoce5Y;
    metrics.promoter_holding_change_1q = promoterStats.latestChange;
    metrics.promoter_holding_change_4q = promoterStats.netChange;
    metrics.promoter_net_change_4q = promoterStats.netChange;
    metrics.promoter_max_quarter_drop = promoterStats.maxQuarterDrop;
    metrics.promoter_trend = classifyNetTrend(promoterStats.netChange);
    metrics.fii_holding_change_1q = fiiStats.latestChange;
    metrics.fii_holding_change_4q = fiiStats.netChange;
    metrics.fii_trend = classifyNetTrend(fiiStats.netChange);
    metrics.dii_holding_change_1q = diiStats.latestChange;
    metrics.dii_holding_change_4q = diiStats.netChange;
    metrics.dii_trend = classifyNetTrend(diiStats.netChange);
    metrics.public_holding_change_1q = publicStats.latestChange;
    metrics.public_holding_change_4q = publicStats.netChange;
    metrics.public_trend = classifyPublicTrend(publicStats.netChange);
    metrics.roe_change =
      getLatest(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"])) !== null &&
      getPrevious(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"])) !== null
        ? getLatest(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"])) -
          getPrevious(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"]))
        : null;
    metrics.roce_change =
      getLatest(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"])) !== null &&
      getPrevious(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"])) !== null
        ? getLatest(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"])) -
          getPrevious(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"]))
        : null;
    metrics.debt_to_equity_change =
      getLatest(ratioRows, "debt_to_equity") !== null && getPrevious(ratioRows, "debt_to_equity") !== null
        ? getLatest(ratioRows, "debt_to_equity") - getPrevious(ratioRows, "debt_to_equity")
        : null;
    metrics.opm_change =
      getLatest(profitRows, (item) => getNumberFromObject(item, ["opm_percent", "operating_profit_margin"])) !== null &&
      getPrevious(profitRows, (item) => getNumberFromObject(item, ["opm_percent", "operating_profit_margin"])) !== null
        ? getLatest(profitRows, (item) => getNumberFromObject(item, ["opm_percent", "operating_profit_margin"])) -
          getPrevious(profitRows, (item) => getNumberFromObject(item, ["opm_percent", "operating_profit_margin"]))
        : null;
    metrics.interest_coverage_change =
      getLatest(ratioRows, (item) => getNumberFromObject(item, ["interest_coverage", "interest_coverage_ratio"])) !== null &&
      getPrevious(ratioRows, (item) => getNumberFromObject(item, ["interest_coverage", "interest_coverage_ratio"])) !== null
        ? getLatest(ratioRows, (item) => getNumberFromObject(item, ["interest_coverage", "interest_coverage_ratio"])) -
          getPrevious(ratioRows, (item) => getNumberFromObject(item, ["interest_coverage", "interest_coverage_ratio"]))
        : null;
    metrics.debtor_days_change =
      getLatest(ratioRows, (item) => getNumberFromObject(item, ["debtor_days", "receivable_days"])) !== null &&
      getPrevious(ratioRows, (item) => getNumberFromObject(item, ["debtor_days", "receivable_days"])) !== null
        ? getLatest(ratioRows, (item) => getNumberFromObject(item, ["debtor_days", "receivable_days"])) -
          getPrevious(ratioRows, (item) => getNumberFromObject(item, ["debtor_days", "receivable_days"]))
        : null;
    metrics.inventory_days_change =
      getLatest(ratioRows, "inventory_days") !== null && getPrevious(ratioRows, "inventory_days") !== null
        ? getLatest(ratioRows, "inventory_days") - getPrevious(ratioRows, "inventory_days")
        : null;
    metrics.working_capital_days_change =
      getLatest(ratioRows, "working_capital_days") !== null && getPrevious(ratioRows, "working_capital_days") !== null
        ? getLatest(ratioRows, "working_capital_days") - getPrevious(ratioRows, "working_capital_days")
        : null;
    metrics.profit_positive_last_3_years = countPositiveLastN(
      profitRows,
      (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"]),
      3,
    );
    metrics.operating_cash_flow_positive_last_3_years = countPositiveLastN(
      cashRows,
      (item) => getNumberFromObject(item, ["cash_from_operating_activity", "cash_from_operating_activities", "operating_cash_flow"]),
      3,
    );
    metrics.dividend_paying_last_3_years =
      dividendHistory.length >= 3 ? dividendHistory.slice(-3).every((value) => value > 0) : (metrics.dividend_yield ?? 0) > 0;
    metrics.sales_growth_consistency = (() => {
      const values = cleanSeries(profitRows, (item) => getNumberFromObject(item, ["sales", "revenue"]));
      if (values.length < 4) return false;
      const growths = values.slice(1).map((value, index) => getGrowthPercent(values[index], value));
      return growths.slice(-3).every((value) => value !== null && value > 0);
    })();
    metrics.profit_growth_consistency = (() => {
      const values = cleanSeries(profitRows, (item) => getNumberFromObject(item, ["net_profit", "profit_after_tax", "pat"]));
      if (values.length < 4) return false;
      const growths = values.slice(1).map((value, index) => getGrowthPercent(values[index], value));
      return growths.slice(-3).every((value) => value !== null && value > 0);
    })();
    metrics.margin_stability = isStableRange(
      cleanSeries(profitRows, (item) => getNumberFromObject(item, ["opm_percent", "operating_profit_margin"])).slice(-4),
      5,
    );
    metrics.roe_stability = isStableRange(cleanSeries(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"])).slice(-4), 5);
    metrics.roce_stability = isStableRange(cleanSeries(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"])).slice(-4), 5);
  }

  return {
    ...metrics,
    ...eodMetrics,
  };
};

const buildFastSearchMetrics = (row = {}, activeRow = null, candles = [], options = {}) => {
  const valueMetrics = row.value_metrics || {};
  const latestProfit = Array.isArray(row.profit_loss_history) ? row.profit_loss_history[row.profit_loss_history.length - 1] || {} : {};
  const latestRatio = Array.isArray(row.ratio_history) ? row.ratio_history[row.ratio_history.length - 1] || {} : {};
  const latestShare = Array.isArray(row.shareholding_history) ? row.shareholding_history[row.shareholding_history.length - 1] || {} : {};
  const latestBalance = Array.isArray(row.balance_history) ? row.balance_history[row.balance_history.length - 1] || {} : {};
  const latestCash = Array.isArray(row.cash_flow_history) ? row.cash_flow_history[row.cash_flow_history.length - 1] || {} : {};
  const requestedMetricKeys = options.requestedMetricKeys instanceof Set ? options.requestedMetricKeys : null;
  const eodMetrics = buildEodMetrics(candles);
  const currentPrice = eodMetrics.eod_close ?? toNumber(row.current_price ?? row.currentPrice ?? row.cmp ?? row.price);

  const latestRoe = valueMetrics.roe ?? toNumber(latestRatio.roe_percent ?? latestRatio.roe);
  const latestRoce = valueMetrics.roce ?? toNumber(latestRatio.roce_percent ?? latestRatio.roce);
  const latestDebtToEquity = valueMetrics.debt_to_equity ?? toNumber(latestRatio.debt_to_equity);
  const latestOpm = valueMetrics.opm_percent ?? toNumber(latestProfit.opm_percent ?? latestProfit.operating_profit_margin);
  const latestInterestCoverage = valueMetrics.interest_coverage ?? toNumber(latestRatio.interest_coverage ?? latestRatio.interest_coverage_ratio);
  const latestDebtorDays = toNumber(latestRatio.debtor_days ?? latestRatio.receivable_days);
  const latestWorkingCapitalDays = toNumber(latestRatio.working_capital_days);
  const latestCashConversionCycle =
    toNumber(latestRatio.cash_conversion_cycle) ??
    (latestRatio.inventory_days !== undefined &&
    latestDebtorDays !== null &&
    toNumber(latestRatio.days_payable ?? latestRatio.payable_days) !== null
      ? toNumber(latestRatio.inventory_days) + latestDebtorDays - toNumber(latestRatio.days_payable ?? latestRatio.payable_days)
      : null);

  const bookValue = toNumber(row.book_value ?? row.bookValue);
  const marketCap = toNumber(row.market_cap ?? row.marketCap);
  const faceValue = toNumber(row.face_value ?? row.faceValue);
  const totalAssets = toNumber(latestBalance.total_assets);
  const totalLiabilities = toNumber(latestBalance.total_liabilities);
  const reserves = toNumber(latestBalance.reserves);
  const equityCapital = toNumber(latestBalance.equity_capital);
  const netWorth =
    toNumber(latestBalance.net_worth) ??
    (totalAssets !== null && totalLiabilities !== null
      ? totalAssets - totalLiabilities
      : reserves !== null && equityCapital !== null
        ? reserves + equityCapital
        : null);

  const sales = toNumber(latestProfit.sales ?? latestProfit.revenue);
  const revenue = toNumber(latestProfit.revenue ?? latestProfit.sales);
  const operatingProfit = toNumber(latestProfit.operating_profit ?? latestProfit.ebit);
  const netProfit = toNumber(latestProfit.net_profit ?? latestProfit.profit_after_tax ?? latestProfit.pat);
  const eps = toNumber(latestProfit.eps ?? latestProfit.net_profit_profit_for_eps);

  const priceVs = (dmaValue) => (currentPrice !== null && dmaValue !== null && dmaValue !== 0 ? ((currentPrice - dmaValue) / dmaValue) * 100 : null);
  const week52High = eodMetrics.week_52_high ?? eodMetrics.eod_52_week_high ?? null;
  const week52Low = eodMetrics.week_52_low ?? eodMetrics.eod_52_week_low ?? null;
  const allTimeHigh = eodMetrics.all_time_high ?? null;

  const metrics = {
    symbol: row.symbol || activeRow?.symbol || null,
    company_name: row.name || row.company_name || activeRow?.name || null,
    market_cap: marketCap,
    current_price: currentPrice,
    ltp: currentPrice,
    price_to_earning: valueMetrics.pe_ratio ?? toNumber(row.stock_pe ?? row.pe_ratio),
    price_to_book:
      valueMetrics.price_to_book ??
      (currentPrice !== null && bookValue ? currentPrice / bookValue : null),
    ev_ebitda: valueMetrics.ev_ebitda ?? null,
    roe: latestRoe,
    roce: latestRoce,
    opm: latestOpm,
    operating_profit_margin: latestOpm,
    opm_change: null,
    profit_positive_last_3_years: null,
    debt_to_equity: latestDebtToEquity,
    interest_coverage: latestInterestCoverage,
    debtor_days: latestDebtorDays,
    working_capital_days: latestWorkingCapitalDays,
    promoter_holding: valueMetrics.promoters ?? toNumber(latestShare.promoters),
    promoter_holding_change_4q: null,
    fii_holding: valueMetrics.fiis ?? toNumber(latestShare.fiis),
    fii_holding_change_4q: null,
    dii_holding: valueMetrics.diis ?? toNumber(latestShare.diis),
    dividend_yield: valueMetrics.dividend_yield ?? toNumber(latestProfit.dividend_yield),
    sales,
    net_profit: netProfit,
    eps,
    book_value: bookValue,
    lower_circuit: toNumber(row.lower_circuit) ?? toNumber(activeRow?.lowerCircuit),
    upper_circuit: toNumber(row.upper_circuit) ?? toNumber(activeRow?.upperCircuit),
    percent_change: eodMetrics.return_1d,
    eod_close: currentPrice,
    eod_volume: eodMetrics.eod_volume,
    week_52_high: week52High,
    week_52_low: week52Low,
    week_52_high_breakout: eodMetrics.week_52_high_breakout ?? null,
    distance_from_52_week_high_percent:
      currentPrice !== null && week52High !== null && week52High > 0 ? ((week52High - currentPrice) / week52High) * 100 : null,
    all_time_high: allTimeHigh,
    all_time_high_breakout: eodMetrics.all_time_high_breakout ?? null,
    avg_volume_20d: eodMetrics.avg_volume_20d ?? null,
    avg_traded_value_20d: eodMetrics.avg_traded_value_20d ?? null,
    traded_days_20d: eodMetrics.traded_days_20d ?? null,
    volume_spike_20d:
      eodMetrics.avg_volume_20d && eodMetrics.eod_volume !== null ? eodMetrics.eod_volume / eodMetrics.avg_volume_20d : null,
    volatility_20d: eodMetrics.volatility_20d ?? null,
    is_liquid: eodMetrics.is_liquid ?? null,
    dma_50: eodMetrics.dma_50 ?? null,
    dma_200: eodMetrics.dma_200 ?? null,
    dma_50_vs_dma_200:
      eodMetrics.dma_50 !== null && eodMetrics.dma_200 !== null ? eodMetrics.dma_50 - eodMetrics.dma_200 : null,
    price_vs_dma_50_percent: priceVs(eodMetrics.dma_50),
    close_above_50_dma: currentPrice !== null && eodMetrics.dma_50 !== null ? currentPrice > eodMetrics.dma_50 : null,
    close_above_200_dma: currentPrice !== null && eodMetrics.dma_200 !== null ? currentPrice > eodMetrics.dma_200 : null,
    atr_14: eodMetrics.atr_14 ?? null,
    rsi_14: eodMetrics.rsi_14 ?? null,
    macd_line: eodMetrics.macd_line ?? null,
    macd_signal: eodMetrics.macd_signal ?? null,
    adx_14: eodMetrics.adx_14 ?? null,
    supertrend_signal: eodMetrics.supertrend_signal ?? null,
    return_1m: eodMetrics.return_1m ?? null,
    return_3m: eodMetrics.return_3m ?? null,
    return_6m: eodMetrics.return_6m ?? null,
    return_1y: eodMetrics.return_1y ?? null,
    return_1w: eodMetrics.return_1w ?? null,
  };

  return metrics;
};

const needsHistoricalMetrics = (preparedClauses = []) =>
  preparedClauses.some(
    (clause) =>
      HISTORY_REQUIRED_FIELDS.has(clause?.field?.key) ||
      HISTORY_REQUIRED_FIELDS.has(clause?.expectedField?.key),
  );

const getRequiredEodHistoryLimit = (preparedClauses = []) =>
  preparedClauses.reduce((maxLimit, clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    const clauseLimit = keys.reduce((limit, key) => Math.max(limit, EOD_HISTORY_LIMIT_BY_FIELD[key] || 1), 1);
    return Math.max(maxLimit, clauseLimit);
  }, 1);

const canUseFastSearchMode = (preparedClauses = []) =>
  !preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => HISTORY_REQUIRED_FIELDS.has(key) || SLOW_SEARCH_FIELDS.has(key));
  });

const enrichRowForSearch = (row, activeByMasterId, candlesByMasterId, fastMode = false, requestedMetricKeys = null) => {
  const masterId = Number(row?.master_id);
  const activeRow = activeByMasterId.get(masterId) || null;
  const candles = candlesByMasterId.get(masterId) || [];
  const searchMetrics = fastMode
    ? buildFastSearchMetrics(row, activeRow, candles, { fastMode, requestedMetricKeys })
    : buildSearchMetrics(row, activeRow, candles, { fastMode });

  return {
    ...row,
    company_name: row?.name || row?.company_name || activeRow?.name || null,
    active_snapshot: activeRow,
    eod_snapshot: candles[candles.length - 1] || null,
    search_metrics: searchMetrics,
    value_metrics: {
      ...(row?.value_metrics || {}),
      promoters: searchMetrics.promoter_holding,
      fiis: searchMetrics.fii_holding,
      diis: searchMetrics.dii_holding,
      public: searchMetrics.public_holding,
      roe: searchMetrics.roe,
      roce: searchMetrics.roce,
      debt_to_equity: searchMetrics.debt_to_equity,
      revenue_cagr_3y: searchMetrics.sales_growth_3y,
      profit_cagr_3y: searchMetrics.profit_growth_3y,
      eps_cagr_3y: searchMetrics.eps_growth_3y,
      opm_percent: searchMetrics.operating_profit_margin,
      dividend_yield: searchMetrics.dividend_yield,
      pe_ratio: searchMetrics.pe_ratio,
      price_to_book: searchMetrics.price_to_book,
      pe_vs_industry: searchMetrics.pe_vs_industry,
      ev_ebitda: searchMetrics.ev_ebitda,
      interest_coverage: searchMetrics.interest_coverage,
      debtor_days: searchMetrics.debtor_days,
      price_to_sales: searchMetrics.price_to_sales,
      company_age_years: searchMetrics.company_age_years,
    },
  };
};

const getSearchUniverseRows = async (
  { asOfDate = null, masterIds = null, splitHistory = false, eodHistoryLimit = 1, fastMode = false } = {},
  db = pool,
) => {
  const allowedMasterIds = Array.isArray(masterIds)
    ? new Set(masterIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))
    : null;

  let baseRows = (await buildValueAnalysisRows({ tier1Only: false, asOfDate }, db)).filter((row) => {
    if (!allowedMasterIds) return true;
    return allowedMasterIds.has(Number(row.master_id));
  });

  if (!baseRows.length) {
    const masterRows = await stockMasterService.getAllMasterStocks();
    const candidateMasters = masterRows.filter((row) => {
      const masterId = Number(row?.id);
      if (!Number.isFinite(masterId) || masterId <= 0) return false;
      if (allowedMasterIds && !allowedMasterIds.has(masterId)) return false;
      return row?.is_active === true && String(row?.screener_status || "").toUpperCase() === "VALID";
    });

    const fallbackMasterIds = candidateMasters.map((row) => Number(row.id));
    const { rows: fundamentalsRows } = await db.query(
      `
        SELECT *
        FROM stock_screener_fundamentals
        WHERE master_id = ANY($1::bigint[])
      `,
      [fallbackMasterIds],
    );
    const fundamentalsByMasterId = new Map(fundamentalsRows.map((row) => [Number(row.master_id), row]));

    baseRows = candidateMasters.map((row) => {
      const masterId = Number(row.id);
      const snapshot = fundamentalsByMasterId.get(masterId) || null;
      const rawHistories = snapshot
        ? {
            ratios: buildPeriodRows("ratios", snapshot, { id: masterId }, snapshot.active_stock_id),
            profit_loss: buildPeriodRows("profit_loss", snapshot, { id: masterId }, snapshot.active_stock_id),
            cash_flow: buildPeriodRows("cash_flow", snapshot, { id: masterId }, snapshot.active_stock_id),
            balance_sheet: buildPeriodRows("balance_sheet", snapshot, { id: masterId }, snapshot.active_stock_id),
            shareholding: buildPeriodRows("shareholdings", snapshot, { id: masterId }, snapshot.active_stock_id),
          }
        : {
            ratios: [],
            profit_loss: [],
            cash_flow: [],
            balance_sheet: [],
            shareholding: [],
          };

      return {
        master_id: masterId,
        symbol: row.symbol || null,
        name: row.name || snapshot?.company || null,
        company_name: snapshot?.company || row.name || null,
        exchange: row.exchange || null,
        market_cap: null,
        current_price: null,
        analysis: null,
        analysis_metrics: {},
        value_metrics: {},
        ratio_history: rawHistories.ratios,
        profit_loss_history: rawHistories.profit_loss,
        cash_flow_history: rawHistories.cash_flow,
        balance_history: rawHistories.balance_sheet,
        shareholding_history: rawHistories.shareholding,
        raw_histories: rawHistories,
      };
    });
  }

  const universeMasterIds = baseRows.map((row) => Number(row.master_id)).filter((value) => Number.isFinite(value) && value > 0);
  const [activeRows, candleRows] = await Promise.all([
    activeStocksRepo.listByMasterIds(universeMasterIds, db),
    eodHistoryLimit > 1
      ? eodRepo.listRecentCandlesByMasterIds(universeMasterIds, { limitPerMaster: eodHistoryLimit, asOfDate }, db)
      : eodRepo.getLatestCandleRowsByMasterIds(universeMasterIds, asOfDate, db),
  ]);

  const activeByMasterId = new Map(activeRows.map((row) => [Number(row.master_id), row]));
  const candlesByMasterId = new Map(
    Object.entries(groupByMasterId(candleRows)).map(([key, rows]) => [Number(key), rows]),
  );

  return baseRows.map((row) => enrichRowForSearch(row, activeByMasterId, candlesByMasterId, fastMode));
};

const fetchSplitRowsByMasterIds = async (tableName, masterIds = [], db = pool, columns = "*") => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const { rows } = await db.query(
    `
      SELECT ${columns}
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id ASC, period_numeric ASC, id ASC
    `,
    [ids],
  );

  return rows;
};

const fetchLatestSplitRowsByMasterIds = async (tableName, masterIds = [], db = pool, columns = "*") => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const { rows } = await db.query(
    `
      SELECT DISTINCT ON (master_id)
        ${columns}
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id ASC, period_numeric DESC NULLS LAST, id DESC
    `,
    [ids],
  );

  return rows;
};

const getSearchUniverseRowsFromSplit = async (
  {
    asOfDate = null,
    masterIds = null,
    queryNeeds = null,
    eodHistoryLimit = 1,
    fastMode = false,
    requestedMetricKeys = null,
  } = {},
  db = pool,
) => {
  const resolvedNeeds = queryNeeds || {
    splitTables: new Set(["profit_loss", "balance_sheet", "cash_flow", "ratios", "shareholding"]),
    historicalSplitTables: new Set(),
    needsEod: true,
    eodHistoryLimit,
    needsActive: true,
  };
  const allowedMasterIds = Array.isArray(masterIds)
    ? new Set(masterIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))
    : null;

  const candidateMasters = await stockMasterService.getEligibleEodSearchMasters(
    allowedMasterIds ? Array.from(allowedMasterIds) : null,
    db,
  );

  const candidateMasterIds = candidateMasters.map((row) => Number(row.id));
  const fetchSplitRows = (tableName, shortKey) => {
    if (!resolvedNeeds.splitTables.has(shortKey)) return Promise.resolve([]);
    const columnsByTable = fastMode
      ? {
          profit_loss:
            "master_id, period_numeric, sales, revenue, net_profit, profit_after_tax, pat, eps, dividend_payout_percent, dividend_payout_ratio, operating_profit, ebit, opm_percent, operating_profit_margin",
          balance_sheet:
            "master_id, period_numeric, total_assets, total_liabilities, reserves, equity_capital, net_worth, cash_equivalents, borrowings, cash_and_cash_equivalents, cash_bank",
          cash_flow:
            "master_id, period_numeric, cash_from_operating_activity, cash_from_operating_activities, operating_cash_flow, net_cash_flow",
          ratios:
            "master_id, period_numeric, roe_percent, roe, roce_percent, roce, debt_to_equity, interest_coverage, interest_coverage_ratio, debtor_days, receivable_days, days_payable, payable_days, inventory_days, working_capital_days, cash_conversion_cycle",
          shareholding:
            "master_id, period_numeric, promoters, fiis, diis, public",
        }
      : null;
    if (resolvedNeeds.historicalSplitTables.has(shortKey)) {
      return fetchSplitRowsByMasterIds(
        tableName,
        candidateMasterIds,
        db,
        columnsByTable?.[shortKey] || "*",
      );
    }
    return fetchLatestSplitRowsByMasterIds(
      tableName,
      candidateMasterIds,
      db,
      columnsByTable?.[shortKey] || "*",
    );
  };
  const [profitRows, balanceRows, cashRows, ratioRows, shareRows, activeRows, candleRows] = await Promise.all([
    fetchSplitRows("stock_fundamental_profit_loss_periods", "profit_loss"),
    fetchSplitRows("stock_fundamental_balance_sheet_periods", "balance_sheet"),
    fetchSplitRows("stock_fundamental_cash_flow_periods", "cash_flow"),
    fetchSplitRows("stock_fundamental_ratios_periods", "ratios"),
    fetchSplitRows("stock_fundamental_shareholding_periods", "shareholding"),
    resolvedNeeds.needsActive ? activeStocksRepo.listByMasterIds(candidateMasterIds, db) : Promise.resolve([]),
    resolvedNeeds.needsEod
      ? fastMode
        ? (resolvedNeeds.eodHistoryLimit > 1
          ? eodRepo.listRecentCandleSnapshotsByMasterIds(
              candidateMasterIds,
              { limitPerMaster: resolvedNeeds.eodHistoryLimit, asOfDate },
              db,
            )
          : eodRepo.getLatestCandleSnapshotsByMasterIds(candidateMasterIds, asOfDate, db))
        : (resolvedNeeds.eodHistoryLimit > 1
          ? eodRepo.listRecentCandlesByMasterIds(candidateMasterIds, { limitPerMaster: resolvedNeeds.eodHistoryLimit, asOfDate }, db)
          : eodRepo.getLatestCandleRowsByMasterIds(candidateMasterIds, asOfDate, db))
      : Promise.resolve([]),
  ]);

  const profitByMaster = groupByMasterId(filterRowsByAsOfDate(profitRows, asOfDate));
  const balanceByMaster = groupByMasterId(filterRowsByAsOfDate(balanceRows, asOfDate));
  const cashByMaster = groupByMasterId(filterRowsByAsOfDate(cashRows, asOfDate));
  const ratioByMaster = groupByMasterId(filterRowsByAsOfDate(ratioRows, asOfDate));
  const shareByMaster = groupByMasterId(filterRowsByAsOfDate(shareRows, asOfDate));
  const activeByMasterId = new Map(activeRows.map((row) => [Number(row.master_id), row]));
  const candlesByMasterId = new Map(
    Object.entries(groupByMasterId(candleRows)).map(([key, rows]) => [Number(key), rows]),
  );

  const baseRows = candidateMasters.map((row) => {
    const masterId = Number(row.id);
    return {
      master_id: masterId,
      symbol: row.symbol || null,
      name: row.name || null,
      company_name: row.name || null,
      exchange: row.exchange || null,
      market_cap: null,
      current_price: null,
      analysis: null,
      analysis_metrics: {},
      value_metrics: {},
      ratio_history: ratioByMaster[String(masterId)] || [],
      profit_loss_history: profitByMaster[String(masterId)] || [],
      cash_flow_history: cashByMaster[String(masterId)] || [],
      balance_history: balanceByMaster[String(masterId)] || [],
      shareholding_history: shareByMaster[String(masterId)] || [],
      raw_histories: {
        ratios: ratioByMaster[String(masterId)] || [],
        profit_loss: profitByMaster[String(masterId)] || [],
        cash_flow: cashByMaster[String(masterId)] || [],
        balance_sheet: balanceByMaster[String(masterId)] || [],
        shareholding: shareByMaster[String(masterId)] || [],
      },
    };
  });

  return baseRows.map((row) => enrichRowForSearch(row, activeByMasterId, candlesByMasterId, fastMode, requestedMetricKeys));
};

const PERIOD_NUMERIC_SORT_SQL = "(split_part(period_numeric, '-', 2)::int * 100 + split_part(period_numeric, '-', 1)::int)";

const fetchLatestSplitSnapshotRowsByMasterIds = async (tableName, masterIds = [], asOfDate = null, columns = "*", db = pool) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((value) => Number.isFinite(value) && value > 0)));
  if (!ids.length) return [];

  const values = [ids];
  let cutoffClause = "";
  if (asOfDate) {
    const cutoff = asOfDateToPeriodNumericValue(asOfDate);
    if (cutoff) {
      values.push(cutoff);
      cutoffClause = `AND (${PERIOD_NUMERIC_SORT_SQL}) <= $${values.length}::int`;
    }
  }

  const { rows } = await db.query(
    `
      SELECT DISTINCT ON (master_id)
        ${columns}
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
        AND period_numeric IS NOT NULL
        ${cutoffClause}
      ORDER BY master_id ASC, ${PERIOD_NUMERIC_SORT_SQL} DESC, id DESC
    `,
    values,
  );

  return rows;
};

const buildFastFundamentalsSnapshotRows = async (
  {
    asOfDate = null,
    masterIds = null,
    preparedClauses = [],
    limit = 50,
  } = {},
  db = pool,
) => {
  const allowedMasterIds = Array.isArray(masterIds)
    ? new Set(masterIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))
    : null;

  const candidateMasters = await stockMasterService.getEligibleEodSearchMasters(
    allowedMasterIds ? Array.from(allowedMasterIds) : null,
    db,
  );

  const candidateMasterIds = candidateMasters.map((row) => Number(row.id));
  if (!candidateMasterIds.length) {
    return [];
  }

  const needsActive = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => FAST_ACTIVE_FIELD_KEYS.has(key));
  });
  const needsEod = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => FAST_EOD_FIELD_KEYS.has(key));
  });
  const needsShareholding = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => ["promoter_holding", "fii_holding", "dii_holding"].includes(key));
  });
  const needsRatios = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => ["roe", "roce", "debt_to_equity", "interest_coverage", "debtor_days", "working_capital_days"].includes(key));
  });
  const needsProfitLoss = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.some((key) => ["sales", "net_profit", "eps", "opm"].includes(key));
  });

  const [eodRows, shareRows, ratioRows, profitRows, activeRows] = await Promise.all([
    needsEod ? eodRepo.getLatestCandleSnapshotsByMasterIds(candidateMasterIds, asOfDate, db) : Promise.resolve([]),
    needsShareholding
      ? fetchLatestSplitSnapshotRowsByMasterIds(
          "stock_fundamental_shareholding_periods",
          candidateMasterIds,
          asOfDate,
          "master_id, period_numeric, promoters, fiis, diis, public, no_of_shareholders",
          db,
        )
      : Promise.resolve([]),
    needsRatios
      ? fetchLatestSplitSnapshotRowsByMasterIds(
          "stock_fundamental_ratios_periods",
          candidateMasterIds,
          asOfDate,
          "master_id, period_numeric, roe_percent, roce_percent, debt_to_equity, interest_coverage, debtor_days, working_capital_days",
          db,
        )
      : Promise.resolve([]),
    needsProfitLoss
      ? fetchLatestSplitSnapshotRowsByMasterIds(
          "stock_fundamental_profit_loss_periods",
          candidateMasterIds,
          asOfDate,
          "master_id, period_numeric, sales, revenue, operating_profit, net_profit, eps, opm_percent",
          db,
        )
      : Promise.resolve([]),
    needsActive ? activeStocksRepo.listByMasterIds(candidateMasterIds, db) : Promise.resolve([]),
  ]);

  const eodByMaster = new Map(eodRows.map((row) => [Number(row.master_id), row]));
  const shareByMaster = groupByMasterId(shareRows);
  const ratioByMaster = groupByMasterId(ratioRows);
  const profitByMaster = groupByMasterId(profitRows);
  const activeByMaster = new Map(activeRows.map((row) => [Number(row.master_id), row]));

  const needsReturn1wHistory = preparedClauses.some((clause) => {
    const keys = [clause?.field?.key, clause?.expectedField?.key].filter(Boolean);
    return keys.includes("return_1w");
  });
  const candleRows = needsReturn1wHistory
    ? await eodRepo.listRecentCandlesByMasterIds(candidateMasterIds, { limitPerMaster: 6, asOfDate }, db)
    : [];
  const candlesByMaster = new Map(
    Object.entries(groupByMasterId(candleRows)).map(([key, rows]) => [Number(key), rows]),
  );

  return candidateMasters.map((master) => {
    const masterId = Number(master.id);
    const eodSnapshot = eodByMaster.get(masterId) || {};
    const shareHistory = shareByMaster[String(masterId)] || [];
    const ratioHistory = ratioByMaster[String(masterId)] || [];
    const profitHistory = profitByMaster[String(masterId)] || [];
    const latestShare = shareHistory[shareHistory.length - 1] || {};
    const latestRatio = ratioHistory[ratioHistory.length - 1] || {};
    const latestProfit = profitHistory[profitHistory.length - 1] || {};
    const activeRow = activeByMaster.get(masterId) || null;
    const candles = candlesByMaster.get(masterId) || [];

    const latestClose = toNumber(eodSnapshot.close ?? eodSnapshot.eod_close ?? null);
    const latestVolume = toNumber(eodSnapshot.volume ?? eodSnapshot.eod_volume ?? null);
    const dma50 = toNumber(eodSnapshot.dma_50);
    const dma200 = toNumber(eodSnapshot.dma_200);
    const week52High = toNumber(eodSnapshot.week_52_high);
    const week52Low = toNumber(eodSnapshot.week_52_low);
    const allTimeHigh = toNumber(eodSnapshot.all_time_high);
    const return1d = toNumber(eodSnapshot.return_1d ?? eodSnapshot.percent_change);
    const return1w = toNumber(eodSnapshot.return_1w) ?? (candles.length > 5 ? getReturnPct(candles, 5) : null);
    const priceVs = (dmaValue) =>
      latestClose !== null && dmaValue !== null && dmaValue !== 0 ? ((latestClose - dmaValue) / dmaValue) * 100 : null;

    const metrics = {
      symbol: master.symbol || null,
      company_name: master.name || null,
      current_price: latestClose,
      eod_close: latestClose,
      ltp: latestClose,
      cmp: latestClose,
      price: latestClose,
      percent_change: return1d,
      return_1d: return1d,
      return_1w: return1w,
      return_1m: toNumber(eodSnapshot.return_1m),
      return_3m: toNumber(eodSnapshot.return_3m),
      return_6m: toNumber(eodSnapshot.return_6m),
      return_1y: toNumber(eodSnapshot.return_1y),
      eod_volume: latestVolume,
      week_52_high: week52High,
      week_52_low: week52Low,
      week_52_high_breakout:
        typeof eodSnapshot.week_52_high_breakout === "boolean"
          ? eodSnapshot.week_52_high_breakout
          : latestClose !== null && week52High !== null
            ? latestClose >= week52High
            : null,
      distance_from_52_week_high_percent:
        latestClose !== null && week52High !== null && week52High > 0 ? ((week52High - latestClose) / week52High) * 100 : null,
      all_time_high: allTimeHigh,
      all_time_high_breakout:
        typeof eodSnapshot.all_time_high_breakout === "boolean"
          ? eodSnapshot.all_time_high_breakout
          : latestClose !== null && allTimeHigh !== null
            ? latestClose >= allTimeHigh
            : null,
      avg_volume_20d: toNumber(eodSnapshot.avg_volume_20d),
      avg_traded_value_20d: toNumber(eodSnapshot.avg_traded_value_20d),
      traded_days_20d: toNumber(eodSnapshot.traded_days_20d),
      volume_spike_20d:
        toNumber(eodSnapshot.avg_volume_20d) && latestVolume !== null
          ? latestVolume / toNumber(eodSnapshot.avg_volume_20d)
          : null,
      volatility_20d: toNumber(eodSnapshot.volatility_20d),
      is_liquid:
        typeof eodSnapshot.is_liquid === "boolean"
          ? eodSnapshot.is_liquid
          : null,
      dma_20: toNumber(eodSnapshot.dma_20),
      dma_50: dma50,
      dma_200: dma200,
      dma_50_vs_dma_200: dma50 !== null && dma200 !== null ? dma50 - dma200 : null,
      price_vs_dma_50_percent: priceVs(dma50),
      price_vs_dma_200_percent: priceVs(dma200),
      close_above_50_dma: latestClose !== null && dma50 !== null ? latestClose > dma50 : null,
      close_above_200_dma: latestClose !== null && dma200 !== null ? latestClose > dma200 : null,
      atr_14: toNumber(eodSnapshot.atr_14),
      rsi_14: toNumber(eodSnapshot.rsi_14),
      macd_line: toNumber(eodSnapshot.macd_line),
      macd_signal: toNumber(eodSnapshot.macd_signal),
      adx_14: toNumber(eodSnapshot.adx_14),
      supertrend_signal:
        eodSnapshot.supertrend_signal === null || eodSnapshot.supertrend_signal === undefined
          ? null
          : Number(eodSnapshot.supertrend_signal),
      promoter_holding: toNumber(latestShare.promoters),
      fii_holding: toNumber(latestShare.fiis),
      dii_holding: toNumber(latestShare.diis),
      roe: toNumber(latestRatio.roe_percent ?? latestRatio.roe),
      roce: toNumber(latestRatio.roce_percent ?? latestRatio.roce),
      opm: toNumber(latestProfit.opm_percent),
      debt_to_equity: toNumber(latestRatio.debt_to_equity),
      interest_coverage: toNumber(latestRatio.interest_coverage),
      debtor_days: toNumber(latestRatio.debtor_days),
      working_capital_days: toNumber(latestRatio.working_capital_days),
      sales: toNumber(latestProfit.sales ?? latestProfit.revenue),
      net_profit: toNumber(latestProfit.net_profit),
      eps: toNumber(latestProfit.eps),
      lower_circuit: toNumber(eodSnapshot.lower_circuit) ?? toNumber(activeRow?.lowerCircuit),
      upper_circuit: toNumber(eodSnapshot.upper_circuit) ?? toNumber(activeRow?.upperCircuit),
    };

    return {
      master_id: masterId,
      symbol: master.symbol || null,
      name: master.name || null,
      company_name: master.name || null,
      market_cap: null,
      current_price: latestClose,
      analysis: {
        score: preparedClauses.length * 10,
        grade: "MATCH",
        recommendation: "All clauses matched",
      },
      analysis_metrics: {},
      value_metrics: {
        promoters: metrics.promoter_holding,
        fiis: metrics.fii_holding,
        diis: metrics.dii_holding,
        public: toNumber(latestShare.public),
        roe: metrics.roe,
        roce: metrics.roce,
        debt_to_equity: metrics.debt_to_equity,
        revenue_cagr_3y: null,
        profit_cagr_3y: null,
        eps_cagr_3y: null,
        opm_percent: metrics.opm,
        dividend_yield: null,
        pe_ratio: null,
        price_to_book: null,
        pe_vs_industry: null,
        ev_ebitda: null,
        interest_coverage: metrics.interest_coverage,
        debtor_days: metrics.debtor_days,
        price_to_sales: null,
        company_age_years: null,
      },
      search_metrics: metrics,
      active_snapshot: activeRow,
      eod_snapshot: eodSnapshot,
      ratio_history: ratioHistory,
      profit_loss_history: profitHistory,
      cash_flow_history: [],
      balance_history: [],
      shareholding_history: shareHistory,
      raw_histories: {
        ratios: ratioHistory,
        profit_loss: profitHistory,
        cash_flow: [],
        balance_sheet: [],
        shareholding: shareHistory,
      },
    };
  });
};

const searchFundamentalsSnapshot = async ({ query = "", limit = 50, asOfDate = null, masterIds = null } = {}, db = pool) => {
  const parsedClauses = parseQuery(query);
  if (!String(query || "").trim()) {
    return { query, parsed: [], total: 0, rows: [], suggestions: [] };
  }

  const preparedClauses = parsedClauses.map((clause) => {
    const field = resolveFieldCandidates(clause.fieldText)[0] || null;
    const expectedField =
      clause.valueText && !/^-?\d+(\.\d+)?$/.test(String(clause.valueText).trim())
        ? resolveFieldCandidates(clause.valueText)[0] || null
        : null;
    return {
      ...clause,
      field,
      expectedField,
      valid: Boolean(field && clause.operator && (clause.valueText !== null || expectedField)),
    };
  });

  if (!isFastFundamentalsQuery(preparedClauses)) {
    const fallback = await searchStocksUsingSplitData({ query, limit, asOfDate, masterIds }, db);
    return {
      ...fallback,
      engine: "split_fundamentals_fallback",
    };
  }

  const rows = await buildFastFundamentalsSnapshotRows({ asOfDate, masterIds, preparedClauses, limit }, db);
  const maxRows = Math.max(1, Math.min(100, Number(limit) || 50));
  const rankedRows = rows
    .map((row) => {
      const matches = preparedClauses.map((clause) => {
        if (!clause.field || !clause.operator) {
          return {
            field: clause.fieldText,
            key: null,
            operator: clause.operator,
            threshold: clause.valueText,
            actual: null,
            formattedActual: "?",
            status: "invalid",
            reason: "Unknown field or incomplete clause",
          };
        }

        const actual = clause.field.getValue(row);
        const matched = compare(clause.field, actual, clause.operator, clause.valueText, clause.expectedField, row);
        const rightActual = clause.expectedField ? clause.expectedField.getValue(row) : null;
        return {
          field: clause.field.label,
          key: clause.field.key,
          operator: clause.operator,
          threshold: clause.expectedField?.label || clause.valueText,
          actual,
          formattedActual: formatValue(clause.field, actual),
          right_actual: rightActual,
          formattedRightActual: clause.expectedField ? formatValue(clause.expectedField, rightActual) : null,
          status: matched ? "match" : "miss",
          reason: matched ? `${clause.field.label} matched` : `${clause.field.label} did not match`,
        };
      });

      const matched = matches.every((item) => item.status === "match");
      return {
        ...row,
        search: {
          query,
          clauses: preparedClauses.map((clause) => ({
            raw: clause.raw,
            fieldText: clause.fieldText,
            operator: clause.operator,
            valueText: clause.valueText,
            key: clause.field?.key || null,
            label: clause.field?.label || null,
            expectedKey: clause.expectedField?.key || null,
            expectedLabel: clause.expectedField?.label || null,
            valid: clause.valid,
          })),
          matches,
          matched,
          matched_count: matches.filter((item) => item.status === "match").length,
        },
      };
    })
    .filter((row) => row.search?.matched)
    .sort((a, b) => {
      const scoreA = Number(a.analysis?.score || 0);
      const scoreB = Number(b.analysis?.score || 0);
      if (scoreB !== scoreA) return scoreB - scoreA;
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    });

  const total = rankedRows.length;
  const matchedRows = rankedRows.slice(0, maxRows);

  return {
    query,
    engine: "eod_snapshot_plus_split",
    parsed: preparedClauses.map((clause) => ({
      raw: clause.raw,
      fieldText: clause.fieldText,
      operator: clause.operator,
      valueText: clause.valueText,
      key: clause.field?.key || null,
      label: clause.field?.label || null,
      expectedKey: clause.expectedField?.key || null,
      expectedLabel: clause.expectedField?.label || null,
      valid: clause.valid,
    })),
    total,
    rows: matchedRows,
    suggestions: suggestSearchFields(query),
  };
};

const makeNumberField = (key, label, aliases = [], example, unit = null) =>
  buildField({
    key,
    label,
    aliases,
    example,
    unit,
    type: "number",
    operators: NUMBER_OPERATORS,
    getValue: fieldValue(key),
  });

const makeTextField = (key, label, aliases = [], example) =>
  buildField({
    key,
    label,
    aliases,
    example,
    type: "text",
    operators: TEXT_OPERATORS,
    getValue: fieldValue(key),
  });

const makeBooleanField = (key, label, aliases = [], example) =>
  buildField({
    key,
    label,
    aliases,
    example,
    type: "boolean",
    operators: BOOLEAN_OPERATORS,
    getValue: fieldValue(key),
  });

const SEARCH_FIELDS = [
  makeTextField("symbol", "Symbol", ["ticker"], "Symbol contains tata"),
  makeTextField("company_name", "Company Name", ["name", "company"], "Company Name contains power"),
  makeNumberField("sales_growth_1y", "Sales growth", ["sales growth 1year", "revenue growth", "sales growth 1y"], "Sales growth > 12", "%"),
  makeNumberField("sales_growth_3y", "Sales growth 3Years", ["sales growth 3y", "revenue cagr 3y"], "Sales growth 3Years > 12", "%"),
  makeNumberField("profit_growth_1y", "Profit growth", ["profit growth 1year", "pat growth"], "Profit growth > 15", "%"),
  makeNumberField("profit_growth_3y", "Profit growth 3Years", ["profit cagr 3y", "pat cagr 3y"], "Profit growth 3Years > 15", "%"),
  makeNumberField("eps_growth_1y", "EPS growth", ["eps growth 1year"], "EPS growth > 10", "%"),
  makeNumberField("return_1w", "1 week return", ["1w return", "1 week return"], "1 week return > 5", "%"),
  makeNumberField("return_1m", "1 month return", ["1m return"], "1 month return > 10", "%"),
  makeNumberField("return_3m", "3 month return", ["3m return"], "3 month return > 15", "%"),
  makeNumberField("return_6m", "6 month return", ["6m return"], "6 month return > 20", "%"),
  makeNumberField("return_1y", "1 year return", ["1y return"], "1 year return > 25", "%"),
  makeNumberField("price_to_earning", "Price to earning", ["pe", "p/e", "pe ratio"], "Price to earning < 30"),
  makeNumberField("price_to_book", "Price to book", ["pb", "p/b", "price to book value"], "Price to book < 3"),
  makeNumberField("ev_ebitda", "EV / EBITDA", ["ev ebitda"], "EV / EBITDA < 12"),
  makeNumberField("roe", "Return on equity", ["roe"], "Return on equity > 15", "%"),
  makeNumberField("roce", "Return on capital employed", ["roce"], "Return on capital employed > 15", "%"),
  makeNumberField("opm", "Operating profit margin", ["operating profit margin", "operating margin", "operating_profit_margin"], "Operating profit margin > 10", "%"),
  makeNumberField("opm_change", "OPM change", ["margin change"], "OPM change > 0", "pp"),
  makeBooleanField("profit_positive_last_3_years", "Profit positive last 3 years", ["profit positive 3y"], "Profit positive last 3 years = yes"),
  makeNumberField("debt_to_equity", "Debt to equity", ["de ratio", "d/e", "debt equity"], "Debt to equity < 0.5", "x"),
  makeNumberField("interest_coverage", "Interest coverage", ["interest coverage ratio"], "Interest coverage > 3", "x"),
  makeNumberField("debtor_days", "Debtor days", ["receivable days"], "Debtor days < 90", "days"),
  makeNumberField("working_capital_days", "Working capital days", [], "Working capital days < 120", "days"),
  makeNumberField("promoter_holding", "Promoter holding", ["promoter"], "Promoter holding > 50", "%"),
  makeNumberField("promoter_holding_change_4q", "Promoter holding change 4Q", ["promoter change 4q", "promoter net change 4q"], "Promoter holding change 4Q > 0", "pp"),
  makeNumberField("fii_holding", "FII holding", ["fii"], "FII holding > 10", "%"),
  makeNumberField("fii_holding_change_4q", "FII holding change 4Q", ["fii change 4q"], "FII holding change 4Q > 0", "pp"),
  makeNumberField("dii_holding", "DII holding", ["dii"], "DII holding > 5", "%"),
  makeNumberField("dividend_yield", "Dividend yield", ["div yield"], "Dividend yield > 2", "%"),
  makeNumberField("sales", "Sales", ["revenue"], "Sales > 100", "Cr"),
  makeNumberField("net_profit", "Net profit", ["profit", "pat"], "Net profit > 10", "Cr"),
  makeNumberField("eps", "EPS", [], "EPS > 5"),
  makeNumberField("book_value", "Book value", [], "Book value > 20", "Rs"),
  makeNumberField("market_cap", "Market Capitalization", ["market cap", "mcap"], "Market Capitalization > 500", "Cr"),
  makeNumberField("lower_circuit", "Lower circuit", [], "Lower circuit > 50", "Rs"),
  makeNumberField("upper_circuit", "Upper circuit", [], "Upper circuit > 50", "Rs"),
  makeNumberField("percent_change", "Percent change", ["return_1d", "1d return", "change %"], "Percent change > 2", "%"),
  makeNumberField("eod_close", "Close price", ["current_price", "ltp", "cmp", "price", "close"], "Close price > 100", "Rs"),
  makeNumberField("eod_volume", "Volume", ["volume"], "Volume > 100000"),
  makeNumberField("week_52_high", "52 week high", ["52w high", "eod_52_week_high"], "52 week high > 100", "Rs"),
  makeNumberField("week_52_low", "52 week low", ["52w low", "eod_52_week_low"], "52 week low > 10", "Rs"),
  makeBooleanField("week_52_high_breakout", "52 week high breakout", ["52w high breakout"], "52 week high breakout = yes"),
  makeNumberField("distance_from_52_week_high_percent", "Distance from 52 week high", ["price_from_52_week_high_percent", "distance from 52 week high", "distance from 52w high", "distance_from_52w_high_pct"], "Distance from 52 week high < 10", "%"),
  makeNumberField("all_time_high", "All time high", ["ath"], "All time high > 100", "Rs"),
  makeBooleanField("all_time_high_breakout", "All time high breakout", ["ath breakout"], "All time high breakout = yes"),
  makeNumberField("avg_volume_20d", "Average volume 20Days", ["average volume 20d", "average_volume_20d"], "Average volume 20Days > 500000"),
  makeNumberField("avg_traded_value_20d", "Average traded value 20Days", ["avg traded value 20d"], "Average traded value 20Days > 1000000", "Rs"),
  makeNumberField("traded_days_20d", "Traded days 20Days", ["traded days 20d"], "Traded days 20Days > 15"),
  makeNumberField("volume_spike_20d", "Volume spike 20Days", ["volume spike"], "Volume spike 20Days > 1.5", "x"),
  makeNumberField("volatility_20d", "Volatility 20Days", ["20 day volatility"], "Volatility 20Days < 5", "%"),
  makeBooleanField("is_liquid", "Is liquid", ["liquid"], "Is liquid = yes"),
  makeNumberField("dma_50", "50 DMA", ["50 ma"], "50 DMA > 100", "Rs"),
  makeNumberField("dma_200", "200 DMA", ["200 ma"], "200 DMA > 100", "Rs"),
  makeNumberField("dma_50_vs_dma_200", "DMA 50 vs DMA 200", ["50 dma vs 200 dma"], "DMA 50 vs DMA 200 > 0", "Rs"),
  makeNumberField("price_vs_dma_50_percent", "Price vs 50 DMA", ["price_vs_dma_50_pct", "distance from 50 dma"], "Price vs 50 DMA > 0", "%"),
  makeBooleanField("close_above_50_dma", "Close above 50 DMA", [], "Close above 50 DMA = yes"),
  makeBooleanField("close_above_200_dma", "Close above 200 DMA", [], "Close above 200 DMA = yes"),
  makeNumberField("atr_14", "ATR 14", ["average true range 14"], "ATR 14 < 10", "Rs"),
  makeNumberField("rsi_14", "RSI 14", ["rsi"], "RSI 14 < 30"),
  makeNumberField("macd_line", "MACD line", ["macd"], "MACD line > 0"),
  makeNumberField("macd_signal", "MACD signal", ["macd signal"], "MACD signal > 0"),
  makeNumberField("adx_14", "ADX 14", ["adx"], "ADX 14 > 20"),
  makeNumberField("supertrend_signal", "Supertrend signal", ["supertrend"], "Supertrend signal > 0"),
];

const suggestSearchFields = (query = "") => {
  const suggestions = resolveFieldCandidates(query).slice(0, 12);
  return suggestions.map((field) => ({
    key: field.key,
    label: field.label,
    aliases: field.aliases,
    example: field.example,
    type: field.type,
    unit: field.unit,
    operators: field.operators,
  }));
};

const searchStocks = async ({ query = "", limit = 50, asOfDate = null, masterIds = null } = {}, db = pool) => {
  const parsedClauses = parseQuery(query);
  if (!String(query || "").trim()) {
    return { query, parsed: [], total: 0, rows: [], suggestions: [] };
  }

  const preparedClauses = parsedClauses.map((clause) => {
    const field = resolveFieldCandidates(clause.fieldText)[0] || null;
    const expectedField =
      clause.valueText && !/^-?\d+(\.\d+)?$/.test(String(clause.valueText).trim())
        ? resolveFieldCandidates(clause.valueText)[0] || null
        : null;
    return {
      ...clause,
      field,
      expectedField,
      valid: Boolean(field && clause.operator && (clause.valueText !== null || expectedField)),
    };
  });

  const splitHistory = needsHistoricalMetrics(preparedClauses);
  const eodHistoryLimit = getRequiredEodHistoryLimit(preparedClauses);
  const fastMode = canUseFastSearchMode(preparedClauses);
  const queryNeeds = getQueryDataNeeds(preparedClauses);
  const requestedMetricKeys = buildRequestedMetricKeys(preparedClauses);
  const rows = await getSearchUniverseRows({ asOfDate, masterIds, splitHistory, eodHistoryLimit, fastMode, queryNeeds }, db);
  const maxRows = Math.max(1, Math.min(100, Number(limit) || 50));
  const rankedRows = rows
    .map((row) => {
      const matches = preparedClauses.map((clause) => {
        if (!clause.field || !clause.operator) {
          return {
            field: clause.fieldText,
            key: null,
            operator: clause.operator,
            threshold: clause.valueText,
            actual: null,
            formattedActual: "?",
            status: "invalid",
            reason: "Unknown field or incomplete clause",
          };
        }

        const actual = clause.field.getValue(row);
        const matched = compare(clause.field, actual, clause.operator, clause.valueText, clause.expectedField, row);
        const rightActual = clause.expectedField ? clause.expectedField.getValue(row) : null;
        return {
          field: clause.field.label,
          key: clause.field.key,
          operator: clause.operator,
          threshold: clause.expectedField?.label || clause.valueText,
          actual,
          formattedActual: formatValue(clause.field, actual),
          right_actual: rightActual,
          formattedRightActual: clause.expectedField ? formatValue(clause.expectedField, rightActual) : null,
          status: matched ? "match" : "miss",
          reason: matched ? `${clause.field.label} matched` : `${clause.field.label} did not match`,
        };
      });

      const matched = matches.every((item) => item.status === "match");
      return {
        ...row,
        market_cap: row.search_metrics?.market_cap ?? row.market_cap ?? null,
        current_price: row.search_metrics?.current_price ?? row.current_price ?? null,
        search: {
          query,
          clauses: preparedClauses.map((clause) => ({
            raw: clause.raw,
            fieldText: clause.fieldText,
            operator: clause.operator,
            valueText: clause.valueText,
            key: clause.field?.key || null,
            label: clause.field?.label || null,
            expectedKey: clause.expectedField?.key || null,
            expectedLabel: clause.expectedField?.label || null,
            valid: clause.valid,
          })),
          matches,
          matched,
          matched_count: matches.filter((item) => item.status === "match").length,
        },
      };
    })
    .filter((row) => row.search?.matched)
    .sort((a, b) => {
      const scoreA = Number(a.analysis?.score || 0);
      const scoreB = Number(b.analysis?.score || 0);
      if (scoreB !== scoreA) return scoreB - scoreA;
      return (toNumber(b.market_cap) || 0) - (toNumber(a.market_cap) || 0);
    });

  const total = rankedRows.length;
  const matchedRows = rankedRows.slice(0, maxRows);

  return {
    query,
    parsed: preparedClauses.map((clause) => ({
      raw: clause.raw,
      fieldText: clause.fieldText,
      operator: clause.operator,
      valueText: clause.valueText,
      key: clause.field?.key || null,
      label: clause.field?.label || null,
      expectedKey: clause.expectedField?.key || null,
      expectedLabel: clause.expectedField?.label || null,
      valid: clause.valid,
    })),
    total,
    rows: matchedRows,
    suggestions: suggestSearchFields(query),
  };
};

const searchStocksUsingSplitData = async ({ query = "", limit = 50, asOfDate = null, masterIds = null, forceFastMode = null } = {}, db = pool) => {
  const parsedClauses = parseQuery(query);
  if (!String(query || "").trim()) {
    return { query, parsed: [], total: 0, rows: [], suggestions: [] };
  }

  const preparedClauses = parsedClauses.map((clause) => {
    const field = resolveFieldCandidates(clause.fieldText)[0] || null;
    const expectedField =
      clause.valueText && !/^-?\d+(\.\d+)?$/.test(String(clause.valueText).trim())
        ? resolveFieldCandidates(clause.valueText)[0] || null
        : null;
    return {
      ...clause,
      field,
      expectedField,
      valid: Boolean(field && clause.operator && (clause.valueText !== null || expectedField)),
    };
  });

  const splitHistory = needsHistoricalMetrics(preparedClauses);
  const eodHistoryLimit = getRequiredEodHistoryLimit(preparedClauses);
  const fastMode = forceFastMode === null ? canUseFastSearchMode(preparedClauses) : Boolean(forceFastMode);
  const queryNeeds = getQueryDataNeeds(preparedClauses);
  const requestedMetricKeys = buildRequestedMetricKeys(preparedClauses);
  const rows = await getSearchUniverseRowsFromSplit({ asOfDate, masterIds, queryNeeds, eodHistoryLimit, fastMode, requestedMetricKeys }, db);
  const maxRows = Math.max(1, Math.min(100, Number(limit) || 50));
  const rankedRows = rows
    .map((row) => {
      const matches = preparedClauses.map((clause) => {
        if (!clause.field || !clause.operator) {
          return {
            field: clause.fieldText,
            key: null,
            operator: clause.operator,
            threshold: clause.valueText,
            actual: null,
            formattedActual: "?",
            status: "invalid",
            reason: "Unknown field or incomplete clause",
          };
        }

        const actual = clause.field.getValue(row);
        const matched = compare(clause.field, actual, clause.operator, clause.valueText, clause.expectedField, row);
        const rightActual = clause.expectedField ? clause.expectedField.getValue(row) : null;
        return {
          field: clause.field.label,
          key: clause.field.key,
          operator: clause.operator,
          threshold: clause.expectedField?.label || clause.valueText,
          actual,
          formattedActual: formatValue(clause.field, actual),
          right_actual: rightActual,
          formattedRightActual: clause.expectedField ? formatValue(clause.expectedField, rightActual) : null,
          status: matched ? "match" : "miss",
          reason: matched ? `${clause.field.label} matched` : `${clause.field.label} did not match`,
        };
      });

      const matched = matches.every((item) => item.status === "match");
      return {
        ...row,
        market_cap: row.search_metrics?.market_cap ?? row.market_cap ?? null,
        current_price: row.search_metrics?.current_price ?? row.current_price ?? null,
        search: {
          query,
          clauses: preparedClauses.map((clause) => ({
            raw: clause.raw,
            fieldText: clause.fieldText,
            operator: clause.operator,
            valueText: clause.valueText,
            key: clause.field?.key || null,
            label: clause.field?.label || null,
            expectedKey: clause.expectedField?.key || null,
            expectedLabel: clause.expectedField?.label || null,
            valid: clause.valid,
          })),
          matches,
          matched,
          matched_count: matches.filter((item) => item.status === "match").length,
        },
      };
    })
    .filter((row) => row.search?.matched)
    .sort((a, b) => {
      const scoreA = Number(a.analysis?.score || 0);
      const scoreB = Number(b.analysis?.score || 0);
      if (scoreB !== scoreA) return scoreB - scoreA;
      return (toNumber(b.market_cap) || 0) - (toNumber(a.market_cap) || 0);
    });

  const total = rankedRows.length;
  const matchedRows = rankedRows.slice(0, maxRows);

  return {
    query,
    parsed: preparedClauses.map((clause) => ({
      raw: clause.raw,
      fieldText: clause.fieldText,
      operator: clause.operator,
      valueText: clause.valueText,
      key: clause.field?.key || null,
      label: clause.field?.label || null,
      expectedKey: clause.expectedField?.key || null,
      expectedLabel: clause.expectedField?.label || null,
      valid: clause.valid,
    })),
    total,
    rows: matchedRows,
    suggestions: suggestSearchFields(query),
  };
};

const searchStocksUsingSplitDataFast = async (params = {}, db = pool) =>
  searchStocksUsingSplitData({ ...(params || {}), forceFastMode: true }, db);

module.exports = {
  SEARCH_FIELDS,
  suggestSearchFields,
  searchStocks,
  searchFundamentalsSnapshot,
  searchStocksUsingSplitData,
  searchStocksUsingSplitDataFast,
};
