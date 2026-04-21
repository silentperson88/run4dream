const { pool } = require("../config/db");
const { buildValueAnalysisRows } = require("./valueAnalysis.service");
const stockMasterService = require("./stockMaster.service");
const activeStocksRepo = require("../repositories/activeStocks.repository");
const eodRepo = require("../repositories/eod.repository");
const { buildPeriodRows } = require("../repositories/fundamentalsSplit.repository");
const { filterRowsByAsOfDate } = require("../utils/asOfDate.utils");

const NUMBER_OPERATORS = [">=", "<=", "!=", "==", "=", ">", "<"];
const TEXT_OPERATORS = ["contains", "starts with", "ends with", "=", "!="];
const BOOLEAN_OPERATORS = ["=", "!="];
const FIELD_PATTERN = /^(.+?)\s*(>=|<=|!=|==|=|>|<|contains|starts with|ends with)\s*(.+)$/i;

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
  const latestOpen = getLatestCandleValue(candles, "open");
  const latestHigh = getLatestCandleValue(candles, "high");
  const latestLow = getLatestCandleValue(candles, "low");
  const latestClose = getLatestCandleValue(candles, "close");
  const latestVolume = getLatestCandleValue(candles, "volume");
  const last252 = candles.slice(Math.max(0, candles.length - 252));
  const dma10 = averageCandleMetric(candles, "close", 10);
  const dma20 = averageCandleMetric(candles, "close", 20);
  const dma50 = averageCandleMetric(candles, "close", 50);
  const dma100 = averageCandleMetric(candles, "close", 100);
  const dma200 = averageCandleMetric(candles, "close", 200);
  const avgVolume20 = averageCandleMetric(candles, "volume", 20);
  const avgVolume50 = averageCandleMetric(candles, "volume", 50);
  const high52w = last252.length ? Math.max(...last252.map((item) => Number(item.high || item.close || 0))) : null;
  const low52w = last252.length ? Math.min(...last252.map((item) => Number(item.low || item.close || 0))) : null;
  const dailyReturns20 = [];

  for (let i = Math.max(1, candles.length - 20); i < candles.length; i += 1) {
    const current = toNumber(candles[i]?.close);
    const previous = toNumber(candles[i - 1]?.close);
    const returnPct = getGrowthPercent(previous, current);
    if (returnPct !== null) dailyReturns20.push(returnPct);
  }

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
    return_1d: getReturnPct(candles, 1),
    return_1w: getReturnPct(candles, 5),
    return_1m: getReturnPct(candles, 21),
    return_3m: getReturnPct(candles, 63),
    return_6m: getReturnPct(candles, 126),
    return_1y: getReturnPct(candles, 252),
    dma_10: dma10,
    dma_20: dma20,
    dma_50: dma50,
    dma_100: dma100,
    dma_200: dma200,
    dma_10_vs_dma_20: dma10 !== null && dma20 !== null ? dma10 - dma20 : null,
    dma_10_vs_dma_50: dma10 !== null && dma50 !== null ? dma10 - dma50 : null,
    dma_10_vs_dma_100: dma10 !== null && dma100 !== null ? dma10 - dma100 : null,
    dma_10_vs_dma_200: dma10 !== null && dma200 !== null ? dma10 - dma200 : null,
    dma_20_vs_dma_50: dma20 !== null && dma50 !== null ? dma20 - dma50 : null,
    dma_20_vs_dma_100: dma20 !== null && dma100 !== null ? dma20 - dma100 : null,
    dma_20_vs_dma_200: dma20 !== null && dma200 !== null ? dma20 - dma200 : null,
    dma_50_vs_dma_100: dma50 !== null && dma100 !== null ? dma50 - dma100 : null,
    dma_50_vs_dma_200: dma50 !== null && dma200 !== null ? dma50 - dma200 : null,
    dma_100_vs_dma_200: dma100 !== null && dma200 !== null ? dma100 - dma200 : null,
    price_vs_dma_10_percent: getGrowthPercent(dma10, latestClose),
    price_vs_dma_20_percent: getGrowthPercent(dma20, latestClose),
    price_vs_dma_50_percent: getGrowthPercent(dma50, latestClose),
    price_vs_dma_100_percent: getGrowthPercent(dma100, latestClose),
    price_vs_dma_200_percent: getGrowthPercent(dma200, latestClose),
    average_volume_20d: avgVolume20,
    average_volume_50d: avgVolume50,
    volume_spike_20d: latestVolume !== null && avgVolume20 ? latestVolume / avgVolume20 : null,
    volatility_20d: stddev(dailyReturns20),
    eod_52_week_high: high52w,
    eod_52_week_low: low52w,
    distance_from_52_week_high_percent:
      latestClose !== null && high52w !== null && high52w > 0 ? ((high52w - latestClose) / high52w) * 100 : null,
    distance_from_52_week_low_percent:
      latestClose !== null && low52w !== null && low52w > 0 ? ((latestClose - low52w) / low52w) * 100 : null,
    close_above_10_dma: latestClose !== null && dma10 !== null ? latestClose > dma10 : null,
    close_above_20_dma: latestClose !== null && dma20 !== null ? latestClose > dma20 : null,
    close_above_50_dma: latestClose !== null && dma50 !== null ? latestClose > dma50 : null,
    close_above_200_dma: latestClose !== null && dma200 !== null ? latestClose > dma200 : null,
  };
};

const fieldValue = (key) => (row) => row?.search_metrics?.[key] ?? null;

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

const buildSearchMetrics = (row = {}, activeRow = null, candles = []) => {
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

  const promoterStats = getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["promoters"])));
  const fiiStats = getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["fiis"])));
  const diiStats = getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["diis"])));
  const publicStats = getSeriesTrend(shareRows.slice(-4).map((item) => getNumberFromObject(item, ["public"])));

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

  const averageRoe3Y = averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"]), 3);
  const averageRoe5Y = averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roe_percent", "roe"]), 5);
  const averageRoce3Y = averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"]), 3);
  const averageRoce5Y = averageLastN(ratioRows, (item) => getNumberFromObject(item, ["roce_percent", "roce"]), 5);
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

  return {
    ...metrics,
    ...eodMetrics,
  };
};

const enrichRowForSearch = (row, activeByMasterId, candlesByMasterId) => {
  const masterId = Number(row?.master_id);
  const activeRow = activeByMasterId.get(masterId) || null;
  const candles = candlesByMasterId.get(masterId) || [];
  const searchMetrics = buildSearchMetrics(row, activeRow, candles);

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

const getSearchUniverseRows = async ({ asOfDate = null, masterIds = null } = {}, db = pool) => {
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
    eodRepo.listRecentCandlesByMasterIds(universeMasterIds, { limitPerMaster: 260, asOfDate }, db),
  ]);

  const activeByMasterId = new Map(activeRows.map((row) => [Number(row.master_id), row]));
  const candlesByMasterId = new Map(
    Object.entries(groupByMasterId(candleRows)).map(([key, rows]) => [Number(key), rows]),
  );

  return baseRows.map((row) => enrichRowForSearch(row, activeByMasterId, candlesByMasterId));
};

const fetchSplitRowsByMasterIds = async (tableName, masterIds = [], db = pool) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const { rows } = await db.query(
    `
      SELECT *
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id ASC, period_numeric ASC, id ASC
    `,
    [ids],
  );

  return rows;
};

const getSearchUniverseRowsFromSplit = async ({ asOfDate = null, masterIds = null } = {}, db = pool) => {
  const allowedMasterIds = Array.isArray(masterIds)
    ? new Set(masterIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))
    : null;

  const masterRows = await stockMasterService.getAllMasterStocks();
  const candidateMasters = masterRows.filter((row) => {
    const masterId = Number(row?.id);
    if (!Number.isFinite(masterId) || masterId <= 0) return false;
    if (allowedMasterIds && !allowedMasterIds.has(masterId)) return false;
    return row?.is_active === true && String(row?.screener_status || "").toUpperCase() === "VALID";
  });

  const candidateMasterIds = candidateMasters.map((row) => Number(row.id));
  const [profitRows, balanceRows, cashRows, ratioRows, shareRows, activeRows, candleRows] = await Promise.all([
    fetchSplitRowsByMasterIds("stock_fundamental_profit_loss_periods", candidateMasterIds, db),
    fetchSplitRowsByMasterIds("stock_fundamental_balance_sheet_periods", candidateMasterIds, db),
    fetchSplitRowsByMasterIds("stock_fundamental_cash_flow_periods", candidateMasterIds, db),
    fetchSplitRowsByMasterIds("stock_fundamental_ratios_periods", candidateMasterIds, db),
    fetchSplitRowsByMasterIds("stock_fundamental_shareholding_periods", candidateMasterIds, db),
    activeStocksRepo.listByMasterIds(candidateMasterIds, db),
    eodRepo.listRecentCandlesByMasterIds(candidateMasterIds, { limitPerMaster: 260, asOfDate }, db),
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

  return baseRows.map((row) => enrichRowForSearch(row, activeByMasterId, candlesByMasterId));
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
  makeNumberField("sales_growth_5y", "Sales growth 5Years", ["sales growth 5y", "revenue cagr 5y"], "Sales growth 5Years > 10", "%"),
  makeNumberField("profit_growth_1y", "Profit growth", ["profit growth 1year", "pat growth"], "Profit growth > 15", "%"),
  makeNumberField("profit_growth_3y", "Profit growth 3Years", ["profit cagr 3y", "pat cagr 3y"], "Profit growth 3Years > 15", "%"),
  makeNumberField("profit_growth_5y", "Profit growth 5Years", ["profit cagr 5y", "pat cagr 5y"], "Profit growth 5Years > 15", "%"),
  makeNumberField("eps_growth_1y", "EPS growth", ["eps growth 1year"], "EPS growth > 10", "%"),
  makeNumberField("eps_growth_3y", "EPS growth 3Years", ["eps cagr 3y"], "EPS growth 3Years > 12", "%"),
  makeNumberField("eps_growth_5y", "EPS growth 5Years", ["eps cagr 5y"], "EPS growth 5Years > 12", "%"),
  makeNumberField("average_roe_3y", "Average ROE 3Years", ["avg roe 3y"], "Average ROE 3Years > 15", "%"),
  makeNumberField("average_roe_5y", "Average ROE 5Years", ["avg roe 5y"], "Average ROE 5Years > 15", "%"),
  makeNumberField("average_roce_3y", "Average ROCE 3Years", ["avg roce 3y"], "Average ROCE 3Years > 15", "%"),
  makeNumberField("average_roce_5y", "Average ROCE 5Years", ["avg roce 5y"], "Average ROCE 5Years > 15", "%"),
  makeNumberField("roe", "Return on equity", ["roe"], "Return on equity > 15", "%"),
  makeNumberField("roce", "Return on capital employed", ["roce"], "Return on capital employed > 15", "%"),
  makeNumberField("debt_to_equity", "Debt to equity", ["de ratio", "d/e", "debt equity"], "Debt to equity < 0.5", "x"),
  makeNumberField("promoter_holding", "Promoter holding", ["promoter"], "Promoter holding > 50", "%"),
  makeNumberField("promoter_holding_change_1q", "Promoter holding change 1Q", ["promoter change 1q"], "Promoter holding change 1Q > 0", "pp"),
  makeNumberField("promoter_holding_change_4q", "Promoter holding change 4Q", ["promoter change 4q", "promoter net change 4q"], "Promoter holding change 4Q > 0", "pp"),
  makeNumberField("promoter_max_quarter_drop", "Promoter max quarter drop", ["promoter drop", "promoter single quarter drop"], "Promoter max quarter drop < 1", "pp"),
  makeTextField("promoter_trend", "Promoter trend", ["promoter holding trend"], "Promoter trend = increasing"),
  makeNumberField("fii_holding", "FII holding", ["fii"], "FII holding > 10", "%"),
  makeNumberField("fii_holding_change_1q", "FII holding change 1Q", ["fii change 1q"], "FII holding change 1Q > 0", "pp"),
  makeNumberField("fii_holding_change_4q", "FII holding change 4Q", ["fii change 4q"], "FII holding change 4Q > 0", "pp"),
  makeTextField("fii_trend", "FII trend", ["fii holding trend"], "FII trend = increasing"),
  makeNumberField("dii_holding", "DII holding", ["dii"], "DII holding > 5", "%"),
  makeNumberField("dii_holding_change_1q", "DII holding change 1Q", ["dii change 1q"], "DII holding change 1Q > 0", "pp"),
  makeNumberField("dii_holding_change_4q", "DII holding change 4Q", ["dii change 4q"], "DII holding change 4Q > 0", "pp"),
  makeTextField("dii_trend", "DII trend", ["dii holding trend"], "DII trend = increasing"),
  makeNumberField("public_holding", "Public holding", ["public"], "Public holding < 40", "%"),
  makeNumberField("public_holding_change_1q", "Public holding change 1Q", ["public change 1q"], "Public holding change 1Q < 1", "pp"),
  makeNumberField("public_holding_change_4q", "Public holding change 4Q", ["public change 4q"], "Public holding change 4Q < 2", "pp"),
  makeTextField("public_trend", "Public trend", ["public holding trend"], "Public trend = stable"),
  makeNumberField("dividend_yield", "Dividend yield", ["div yield"], "Dividend yield > 2", "%"),
  makeNumberField("dividend_payout_ratio", "Dividend payout ratio", ["payout ratio"], "Dividend payout ratio > 20", "%"),
  makeNumberField("operating_profit_margin", "Operating profit margin", ["opm", "operating margin"], "Operating profit margin > 10", "%"),
  makeNumberField("opm_change", "OPM change", ["margin change"], "OPM change > 0", "pp"),
  makeNumberField("price_to_earning", "Price to earning", ["pe", "p/e", "pe ratio"], "Price to earning < 30"),
  makeNumberField("peg_ratio", "PEG Ratio", ["peg"], "PEG Ratio < 1.5"),
  makeNumberField("price_to_book", "Price to book", ["pb", "p/b", "price to book value"], "Price to book < 3"),
  makeNumberField("pe_vs_industry", "P/E vs industry", ["pe vs industry avg"], "P/E vs industry < 0.9"),
  makeNumberField("ev_ebitda", "EV / EBITDA", ["ev ebitda"], "EV / EBITDA < 12"),
  makeNumberField("price_to_sales", "Price to sales", ["p/s", "ps ratio"], "Price to sales < 5"),
  makeNumberField("interest_coverage", "Interest coverage", ["interest coverage ratio"], "Interest coverage > 3", "x"),
  makeNumberField("interest_coverage_change", "Interest coverage change", ["interest cover change"], "Interest coverage change > 0", "x"),
  makeNumberField("debtor_days", "Debtor days", ["receivable days"], "Debtor days < 90", "days"),
  makeNumberField("debtor_days_change", "Debtor days change", ["receivable days change"], "Debtor days change < 20", "days"),
  makeNumberField("inventory_days", "Inventory days", [], "Inventory days < 120", "days"),
  makeNumberField("inventory_days_change", "Inventory days change", [], "Inventory days change < 20", "days"),
  makeNumberField("days_payable", "Days payable", ["payable days"], "Days payable > 20", "days"),
  makeNumberField("working_capital_days", "Working capital days", [], "Working capital days < 120", "days"),
  makeNumberField("working_capital_days_change", "Working capital days change", [], "Working capital days change < 20", "days"),
  makeNumberField("cash_conversion_cycle", "Cash conversion cycle", ["ccc"], "Cash conversion cycle < 90", "days"),
  makeNumberField("market_cap", "Market Capitalization", ["market cap", "mcap"], "Market Capitalization > 500", "Cr"),
  makeNumberField("current_price", "Current price", ["cmp", "price"], "Current price < 50", "Rs"),
  makeNumberField("book_value", "Book value", [], "Book value > 20", "Rs"),
  makeNumberField("face_value", "Face value", [], "Face value = 10", "Rs"),
  makeNumberField("sales", "Sales", ["revenue"], "Sales > 100", "Cr"),
  makeNumberField("operating_profit", "Operating profit", ["ebit"], "Operating profit > 20", "Cr"),
  makeNumberField("net_profit", "Net profit", ["profit", "pat"], "Net profit > 10", "Cr"),
  makeNumberField("eps", "EPS", [], "EPS > 5"),
  makeNumberField("borrowings", "Borrowings", ["debt"], "Borrowings < 500", "Cr"),
  makeNumberField("reserves", "Reserves", [], "Reserves > 100", "Cr"),
  makeNumberField("equity_capital", "Equity capital", [], "Equity capital > 5", "Cr"),
  makeNumberField("cash_equivalents", "Cash equivalents", ["cash"], "Cash equivalents > 20", "Cr"),
  makeNumberField("total_liabilities", "Total liabilities", ["liabilities"], "Total liabilities < 1000", "Cr"),
  makeNumberField("total_assets", "Total assets", ["assets"], "Total assets > 1000", "Cr"),
  makeNumberField("net_worth", "Net worth", [], "Net worth > 200", "Cr"),
  makeNumberField("cash_from_operating_activity", "Cash from operating activity", ["cfo", "operating cash flow"], "Cash from operating activity > 0", "Cr"),
  makeNumberField("net_cash_flow", "Net cash flow", [], "Net cash flow > 0", "Cr"),
  makeNumberField("number_of_shareholders", "Number of shareholders", ["no of shareholders"], "Number of shareholders > 10000"),
  makeNumberField("company_age_years", "Company age", ["listing age", "company age years"], "Company age > 5", "years"),
  makeBooleanField("profit_positive_last_3_years", "Profit positive last 3 years", ["profit positive 3y"], "Profit positive last 3 years = yes"),
  makeBooleanField("operating_cash_flow_positive_last_3_years", "Operating cash flow positive last 3 years", ["ocf positive 3y"], "Operating cash flow positive last 3 years = yes"),
  makeBooleanField("dividend_paying_last_3_years", "Dividend paying last 3 years", ["dividend paying 3y"], "Dividend paying last 3 years = yes"),
  makeBooleanField("margin_stability", "Margin stability", ["opm stability"], "Margin stability = yes"),
  makeBooleanField("roe_stability", "ROE stability", [], "ROE stability = yes"),
  makeBooleanField("roce_stability", "ROCE stability", [], "ROCE stability = yes"),
  makeNumberField("ltp", "LTP", ["last traded price"], "LTP < 100", "Rs"),
  makeNumberField("percent_change", "Percent change", ["change %"], "Percent change > 2", "%"),
  makeNumberField("average_price", "Average price", ["avg price"], "Average price > 100", "Rs"),
  makeNumberField("lower_circuit", "Lower circuit", [], "Lower circuit > 50", "Rs"),
  makeNumberField("upper_circuit", "Upper circuit", [], "Upper circuit > 50", "Rs"),
  makeNumberField("week_52_low", "52 week low", ["52w low"], "52 week low > 10", "Rs"),
  makeNumberField("week_52_high", "52 week high", ["52w high"], "52 week high > 100", "Rs"),
  makeNumberField("price_from_52_week_high_percent", "Price from 52 week high", ["distance from 52 week high"], "Price from 52 week high < 20", "%"),
  makeNumberField("price_from_52_week_low_percent", "Price from 52 week low", ["distance from 52 week low"], "Price from 52 week low < 30", "%"),
  makeNumberField("eod_close", "Close price", ["close"], "Close price > 100", "Rs"),
  makeNumberField("eod_open", "Open price", ["open"], "Open price > 100", "Rs"),
  makeNumberField("eod_high", "High", [], "High > 100", "Rs"),
  makeNumberField("eod_low", "Low", [], "Low > 100", "Rs"),
  makeNumberField("eod_volume", "Volume", [], "Volume > 100000"),
  makeNumberField("return_1d", "1 day return", ["1d return"], "1 day return > 2", "%"),
  makeNumberField("return_1w", "1 week return", ["1w return"], "1 week return > 5", "%"),
  makeNumberField("return_1m", "1 month return", ["1m return"], "1 month return > 10", "%"),
  makeNumberField("return_3m", "3 month return", ["3m return"], "3 month return > 15", "%"),
  makeNumberField("return_6m", "6 month return", ["6m return"], "6 month return > 20", "%"),
  makeNumberField("return_1y", "1 year return", ["1y return"], "1 year return > 25", "%"),
  makeNumberField("dma_10", "10 DMA", ["10 ma"], "10 DMA > 100", "Rs"),
  makeNumberField("dma_20", "20 DMA", ["20 ma"], "20 DMA > 100", "Rs"),
  makeNumberField("dma_50", "50 DMA", ["50 ma"], "50 DMA > 100", "Rs"),
  makeNumberField("dma_100", "100 DMA", ["100 ma"], "100 DMA > 100", "Rs"),
  makeNumberField("dma_200", "200 DMA", ["200 ma"], "200 DMA > 100", "Rs"),
  makeNumberField("dma_10_vs_dma_20", "DMA 10 vs DMA 20", ["10 dma vs 20 dma"], "DMA 10 vs DMA 20 > 0", "Rs"),
  makeNumberField("dma_10_vs_dma_50", "DMA 10 vs DMA 50", ["10 dma vs 50 dma"], "DMA 10 vs DMA 50 > 0", "Rs"),
  makeNumberField("dma_10_vs_dma_100", "DMA 10 vs DMA 100", ["10 dma vs 100 dma"], "DMA 10 vs DMA 100 > 0", "Rs"),
  makeNumberField("dma_10_vs_dma_200", "DMA 10 vs DMA 200", ["10 dma vs 200 dma"], "DMA 10 vs DMA 200 > 0", "Rs"),
  makeNumberField("dma_20_vs_dma_50", "DMA 20 vs DMA 50", ["20 dma vs 50 dma"], "DMA 20 vs DMA 50 > 0", "Rs"),
  makeNumberField("dma_20_vs_dma_100", "DMA 20 vs DMA 100", ["20 dma vs 100 dma"], "DMA 20 vs DMA 100 > 0", "Rs"),
  makeNumberField("dma_20_vs_dma_200", "DMA 20 vs DMA 200", ["20 dma vs 200 dma"], "DMA 20 vs DMA 200 > 0", "Rs"),
  makeNumberField("dma_50_vs_dma_100", "DMA 50 vs DMA 100", ["50 dma vs 100 dma"], "DMA 50 vs DMA 100 > 0", "Rs"),
  makeNumberField("dma_50_vs_dma_200", "DMA 50 vs DMA 200", ["50 dma vs 200 dma"], "DMA 50 vs DMA 200 > 0", "Rs"),
  makeNumberField("dma_100_vs_dma_200", "DMA 100 vs DMA 200", ["100 dma vs 200 dma"], "DMA 100 vs DMA 200 > 0", "Rs"),
  makeNumberField("price_vs_dma_10_percent", "Price vs 10 DMA", ["distance from 10 dma"], "Price vs 10 DMA > 0", "%"),
  makeNumberField("price_vs_dma_20_percent", "Price vs 20 DMA", ["distance from 20 dma"], "Price vs 20 DMA > 0", "%"),
  makeNumberField("price_vs_dma_50_percent", "Price vs 50 DMA", ["distance from 50 dma"], "Price vs 50 DMA > 0", "%"),
  makeNumberField("price_vs_dma_100_percent", "Price vs 100 DMA", ["distance from 100 dma"], "Price vs 100 DMA > 0", "%"),
  makeNumberField("price_vs_dma_200_percent", "Price vs 200 DMA", ["distance from 200 dma"], "Price vs 200 DMA > 0", "%"),
  makeNumberField("average_volume_20d", "Average volume 20Days", ["avg volume 20d"], "Average volume 20Days > 500000"),
  makeNumberField("average_volume_50d", "Average volume 50Days", ["avg volume 50d"], "Average volume 50Days > 500000"),
  makeNumberField("volume_spike_20d", "Volume spike 20Days", ["volume spike"], "Volume spike 20Days > 1.5", "x"),
  makeNumberField("volatility_20d", "Volatility 20Days", ["20 day volatility"], "Volatility 20Days < 5", "%"),
  makeNumberField("eod_52_week_high", "EOD 52 week high", ["eod 52w high"], "EOD 52 week high > 100", "Rs"),
  makeNumberField("eod_52_week_low", "EOD 52 week low", ["eod 52w low"], "EOD 52 week low > 20", "Rs"),
  makeNumberField("distance_from_52_week_high_percent", "Distance from 52 week high", ["distance from 52w high"], "Distance from 52 week high < 10", "%"),
  makeNumberField("distance_from_52_week_low_percent", "Distance from 52 week low", ["distance from 52w low"], "Distance from 52 week low < 20", "%"),
  makeBooleanField("close_above_10_dma", "Close above 10 DMA", [], "Close above 10 DMA = yes"),
  makeBooleanField("close_above_20_dma", "Close above 20 DMA", [], "Close above 20 DMA = yes"),
  makeBooleanField("close_above_50_dma", "Close above 50 DMA", [], "Close above 50 DMA = yes"),
  makeBooleanField("close_above_200_dma", "Close above 200 DMA", [], "Close above 200 DMA = yes"),
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

  const rows = await getSearchUniverseRows({ asOfDate, masterIds }, db);
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

const searchStocksUsingSplitData = async ({ query = "", limit = 50, asOfDate = null, masterIds = null } = {}, db = pool) => {
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

  const rows = await getSearchUniverseRowsFromSplit({ asOfDate, masterIds }, db);
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

module.exports = {
  SEARCH_FIELDS,
  suggestSearchFields,
  searchStocks,
  searchStocksUsingSplitData,
};
