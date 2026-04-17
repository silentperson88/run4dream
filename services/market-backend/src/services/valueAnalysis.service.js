const { pool } = require("../config/db");

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parsePeriodParts = (periodNumeric) => {
  const text = String(periodNumeric || "").trim();
  const match = text.match(/^(\d{2})-(\d{4})$/);
  if (!match) return { month: null, year: null };
  return { month: Number(match[1]), year: Number(match[2]) };
};

const sortByPeriodNumericAsc = (rows = []) =>
  [...rows].sort((a, b) => {
    const aParts = parsePeriodParts(a?.period_numeric);
    const bParts = parsePeriodParts(b?.period_numeric);
    if (aParts.year !== bParts.year) return (aParts.year || 0) - (bParts.year || 0);
    if (aParts.month !== bParts.month) return (aParts.month || 0) - (bParts.month || 0);
    return 0;
  });

const lastN = (rows = [], count = 1) => sortByPeriodNumericAsc(rows).slice(Math.max(0, rows.length - count));

const calculateCagr = (startValue, endValue, years) => {
  const start = toNumber(startValue);
  const end = toNumber(endValue);
  if (start === null || end === null || start <= 0 || end <= 0 || !years || years <= 0) return null;
  return (Math.pow(end / start, 1 / years) - 1) * 100;
};

const formatPercent = (value) => {
  const parsed = toNumber(value);
  return parsed === null ? "n/a" : `${parsed.toFixed(2)}%`;
};

const pickGrade = (score) => {
  if (score >= 85) return "Deep Value Buy";
  if (score >= 70) return "Value Buy";
  if (score >= 55) return "Watch";
  return "Reject";
};

const normalizeGrade = (grade) => String(grade || "").trim().toUpperCase();

const matchesGradeFilter = (row, gradeFilter = "ALL") => {
  const normalized = normalizeGrade(gradeFilter);
  if (!normalized || normalized === "ALL") return true;
  return normalizeGrade(row?.analysis?.grade) === normalized;
};

const matchesScoreFilter = (row, minScore = null) => {
  const numeric = toNumber(minScore);
  if (numeric === null) return true;
  return Number(row?.analysis?.score || 0) >= numeric;
};

const sortByOverallRank = (rows = []) =>
  [...rows].sort((a, b) => {
    const scoreDiff = Number(b?.analysis?.score || 0) - Number(a?.analysis?.score || 0);
    if (scoreDiff !== 0) return scoreDiff;
    const valueDiff = Number(a?.value_metrics?.pe_vs_industry || 999) - Number(b?.value_metrics?.pe_vs_industry || 999);
    if (valueDiff !== 0) return valueDiff;
    return String(a?.symbol || "").localeCompare(String(b?.symbol || ""));
  });

const sortByTierScore = (rows = [], tierKey, secondaryKey = "score") =>
  [...rows].sort((a, b) => {
    const tierDiff = Number(b?.analysis?.tier_scores?.[tierKey] || 0) - Number(a?.analysis?.tier_scores?.[tierKey] || 0);
    if (tierDiff !== 0) return tierDiff;
    const secondaryDiff =
      secondaryKey === "score"
        ? Number(b?.analysis?.score || 0) - Number(a?.analysis?.score || 0)
        : Number(b?.analysis?.[secondaryKey] || 0) - Number(a?.analysis?.[secondaryKey] || 0);
    if (secondaryDiff !== 0) return secondaryDiff;
    return String(a?.symbol || "").localeCompare(String(b?.symbol || ""));
  });

const industryKey = (row = {}) => String(row?.industry || row?.sector || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";

const buildIndustryAvgPeMap = (overviewRows = []) => {
  const buckets = {};
  overviewRows.forEach((row) => {
    const key = industryKey(row);
    const pe = toNumber(row?.stock_pe);
    if (pe === null || pe <= 0) return;
    if (!buckets[key]) buckets[key] = [];
    buckets[key].push(pe);
  });

  const map = {};
  Object.entries(buckets).forEach(([key, values]) => {
    map[key] = avg(values);
  });
  return map;
};

const countNegativeDeltas = (values = []) => {
  const valid = values.filter((value) => value !== null && value !== undefined);
  let count = 0;
  for (let i = 1; i < valid.length; i += 1) {
    if (valid[i] < valid[i - 1]) count += 1;
  }
  return count;
};

const tableExists = async (tableName, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = $1
      LIMIT 1
    `,
    [tableName],
  );
  return rows.length > 0;
};

const fetchLatestRowsByMasterIds = async (tableName, masterIds, selectList, db = pool) => {
  if (!masterIds.length) return [];
  const { rows } = await db.query(
    `
      SELECT DISTINCT ON (master_id)
        ${selectList}
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY
        master_id,
        CASE
          WHEN period_numeric ~ '^[0-9]{2}-[0-9]{4}$' THEN split_part(period_numeric, '-', 2)::int
          ELSE NULL
        END DESC NULLS LAST,
        CASE
          WHEN period_numeric ~ '^[0-9]{2}-[0-9]{4}$' THEN split_part(period_numeric, '-', 1)::int
          ELSE NULL
        END DESC NULLS LAST,
        updated_at DESC NULLS LAST,
        id DESC
    `,
    [masterIds.map(Number)],
  );
  return rows;
};

const fetchAllRowsByMasterIds = async (tableName, masterIds, selectList, db = pool) => {
  if (!masterIds.length) return [];
  const { rows } = await db.query(
    `
      SELECT ${selectList}
      FROM ${tableName}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY
        master_id,
        CASE
          WHEN period_numeric ~ '^[0-9]{2}-[0-9]{4}$' THEN split_part(period_numeric, '-', 2)::int
          ELSE NULL
        END ASC NULLS LAST,
        CASE
          WHEN period_numeric ~ '^[0-9]{2}-[0-9]{4}$' THEN split_part(period_numeric, '-', 1)::int
          ELSE NULL
        END ASC NULLS LAST
    `,
    [masterIds.map(Number)],
  );
  return rows;
};

const groupRowsByMasterId = (rows = []) =>
  rows.reduce((acc, row) => {
    const key = String(row?.master_id || "");
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});

const pickHistoryValue = (row, keys = []) => {
  const list = Array.isArray(keys) ? keys : [keys];
  for (const key of list) {
    const value = toNumber(row?.[key]);
    if (value !== null) return value;
  }
  return null;
};

const buildHistory = (rows = [], keys) =>
  sortByPeriodNumericAsc(rows)
    .map((row) => ({
      period_label: row?.period_label || row?.period || null,
      period_numeric: row?.period_numeric || null,
      value: pickHistoryValue(row, keys),
    }))
    .filter((row) => row.period_numeric);

const avg = (values = []) => {
  const valid = values.map(toNumber).filter((v) => v !== null);
  if (!valid.length) return null;
  return valid.reduce((sum, v) => sum + v, 0) / valid.length;
};

const trendIsStableOrIncreasing = (values = []) => {
  const valid = values.map(toNumber).filter((v) => v !== null);
  if (valid.length < 2) return false;
  const first = valid[0];
  const last = valid[valid.length - 1];
  const minimum = Math.min(...valid);
  return last >= first && minimum >= 0;
};

const trendIsIncreasing = (values = []) => {
  const valid = values.map(toNumber).filter((v) => v !== null);
  if (valid.length < 2) return false;
  const first = valid[0];
  const last = valid[valid.length - 1];
  return last > first;
};

const getRecentSeries = (rows = [], key, count = 4) =>
  sortByPeriodNumericAsc(rows)
    .slice(Math.max(0, rows.length - count))
    .map((row) => toNumber(row?.[key]));

const getSeriesTrend = (series = []) => {
  const valid = Array.isArray(series) ? series.filter((value) => value !== null && value !== undefined) : [];
  if (valid.length < 2 || valid.length !== series.length) {
    return {
      hasEnough: false,
      first: null,
      last: null,
      netChange: null,
      maxQuarterDrop: null,
    };
  }

  let maxQuarterDrop = 0;
  for (let i = 1; i < valid.length; i += 1) {
    const drop = valid[i - 1] - valid[i];
    if (drop > maxQuarterDrop) maxQuarterDrop = drop;
  }

  return {
    hasEnough: true,
    first: valid[0],
    last: valid[valid.length - 1],
    netChange: valid[valid.length - 1] - valid[0],
    maxQuarterDrop,
  };
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
  return "spike";
};

const scoreBand = (value, bands) => {
  if (value === null) return { score: 0, reason: "No data" };
  for (const band of bands) {
    if (value >= band.min) return { score: band.score, reason: band.reason };
  }
  return { score: 0, reason: "Weak" };
};

const buildValueAnalysisRows = async ({ tier1Only = true } = {}, db = pool) => {
  try {
  if (!(await tableExists("stock_fundamental_overview", db))) return [];

  const overviewRes = await db.query(
    `
      SELECT
        sm.id AS master_id,
        sm.symbol,
        sm.name,
        sm.exchange,
        NULL::text AS industry,
        NULL::text AS sector,
        sm.created_at AS master_created_at,
        sm.screener_status,
        o.active_stock_id,
        o.company_name,
        o.market_cap,
        o.current_price,
        o.high_low,
        o.stock_pe,
        o.book_value,
        o.dividend_yield,
        o.roce,
        o.roe,
        o.face_value,
        o.pros,
        o.cons,
        o.links,
        o.last_updated_at AS overview_last_updated_at
      FROM stock_master sm
      INNER JOIN stock_fundamental_overview o ON o.master_id = sm.id
      WHERE sm.is_active = TRUE
        AND UPPER(COALESCE(sm.screener_status, '')) = 'VALID'
      ORDER BY COALESCE(o.stock_pe, 0) ASC, COALESCE(o.book_value, 0) ASC, sm.symbol ASC
    `,
  );

  const overviewRows = overviewRes.rows || [];
  const masterIds = overviewRows.map((row) => Number(row.master_id)).filter((value) => Number.isFinite(value));
  if (!masterIds.length) return [];

  const [ratioExists, profitExists, cashExists, balanceExists, shareExists] = await Promise.all([
    tableExists("stock_fundamental_ratios_periods", db),
    tableExists("stock_fundamental_profit_loss_periods", db),
    tableExists("stock_fundamental_cash_flow_periods", db),
    tableExists("stock_fundamental_balance_sheet_periods", db),
    tableExists("stock_fundamental_shareholding_periods", db),
  ]);

  const [ratiosRows, profitRows, cashRows, balanceRows, shareRows] = await Promise.all([
    ratioExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_ratios_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, debtor_days, inventory_days, days_payable, cash_conversion_cycle, working_capital_days, roce_percent, roe_percent, last_updated_at, id, updated_at",
          db,
        )
      : [],
    profitExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_profit_loss_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, sales, revenue, operating_profit, opm_percent, net_profit, eps, net_profit_profit_for_eps, dividend_payout_percent, sales_yoy_growth_percent, interest, depreciation, last_updated_at, id, updated_at",
          db,
        )
      : [],
    cashExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_cash_flow_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, cash_from_operating_activity, net_cash_flow, last_updated_at, id, updated_at",
          db,
        )
      : [],
    balanceExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_balance_sheet_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, borrowing, borrowings, reserves, equity_capital, total_liabilities, total_assets, cash_equivalents, last_updated_at",
          db,
        )
      : [],
    shareExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_shareholding_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, promoters, fiis, diis, public, government, others, no_of_shareholders, last_updated_at, id, updated_at",
          db,
        )
      : [],
  ]);

  const ratiosByMaster = groupRowsByMasterId(ratiosRows);
  const profitByMaster = groupRowsByMasterId(profitRows);
  const cashByMaster = groupRowsByMasterId(cashRows);
  const balanceByMaster = groupRowsByMasterId(balanceRows);
  const shareByMaster = groupRowsByMasterId(shareRows);
  const industryAvgPeMap = buildIndustryAvgPeMap(overviewRows);

  return overviewRows.map((row) => {
    const masterId = String(row.master_id);
    const ratioHistory = sortByPeriodNumericAsc(ratiosByMaster[masterId] || []);
    const profitHistory = sortByPeriodNumericAsc(profitByMaster[masterId] || []);
    const cashHistory = sortByPeriodNumericAsc(cashByMaster[masterId] || []);
    const balanceHistory = sortByPeriodNumericAsc(balanceByMaster[masterId] || []);
    const shareHistory = sortByPeriodNumericAsc(shareByMaster[masterId] || []);

    const latestRatio = ratioHistory[ratioHistory.length - 1] || null;
    const previousRatio = ratioHistory.length > 1 ? ratioHistory[ratioHistory.length - 2] || null : null;
    const latestProfit = profitHistory[profitHistory.length - 1] || null;
    const previousProfit = profitHistory.length > 1 ? profitHistory[profitHistory.length - 2] || null : null;
    const latestCash = cashHistory[cashHistory.length - 1] || null;
    const latestBalance = balanceHistory[balanceHistory.length - 1] || null;
    const previousBalance = balanceHistory.length > 1 ? balanceHistory[balanceHistory.length - 2] || null : null;
    const latestShare = shareHistory[shareHistory.length - 1] || null;
    const previousShare = shareHistory.length > 1 ? shareHistory[shareHistory.length - 2] || null : null;

    const revenueHistory = buildHistory(profitHistory, ["revenue", "sales"]);
    const profitHistoryValues = buildHistory(profitHistory, ["net_profit"]);
    const epsHistory = buildHistory(profitHistory, ["eps", "net_profit_profit_for_eps"]);
    const opmHistory = buildHistory(profitHistory, ["opm_percent"]);
    const cashOcfHistory = buildHistory(cashHistory, ["cash_from_operating_activity"]);
    const inventoryDaysHistory = buildHistory(ratioHistory, ["inventory_days"]);
    const workingCapDaysHistory = buildHistory(ratioHistory, ["working_capital_days"]);
    const debtorDaysHistory = buildHistory(ratioHistory, ["debtor_days"]);
    const payoutHistory = buildHistory(profitHistory, ["dividend_payout_percent"]);

    const revenueCagr3y =
      revenueHistory.length >= 4
        ? calculateCagr(revenueHistory[0]?.value, revenueHistory[revenueHistory.length - 1]?.value, 3)
        : null;
    const profitCagr3y =
      profitHistoryValues.length >= 4
        ? calculateCagr(profitHistoryValues[0]?.value, profitHistoryValues[profitHistoryValues.length - 1]?.value, 3)
        : null;
    const epsCagr3y =
      epsHistory.length >= 4 ? calculateCagr(epsHistory[0]?.value, epsHistory[epsHistory.length - 1]?.value, 3) : null;

    const roe = toNumber(latestRatio?.roe_percent ?? row?.roe);
    const roce = toNumber(latestRatio?.roce_percent ?? row?.roce);
    const borrowings = toNumber(latestBalance?.borrowings ?? latestBalance?.borrowing);
    const previousBorrowings = toNumber(previousBalance?.borrowings ?? previousBalance?.borrowing);
    const reserves = toNumber(latestBalance?.reserves);
    const equityCapital = toNumber(latestBalance?.equity_capital);
    const cashEquivalents = toNumber(latestBalance?.cash_equivalents);
    const totalLiabilities = toNumber(latestBalance?.total_liabilities);
    const marketCap = toNumber(row?.market_cap);
    const currentPrice = toNumber(row?.current_price);
    const bookValue = toNumber(row?.book_value);
    const dividendYield = toNumber(row?.dividend_yield);
    const peRatio = toNumber(row?.stock_pe);
    const promoters = toNumber(latestShare?.promoters ?? row?.promoters);
    const fiis = toNumber(latestShare?.fiis ?? row?.fiis);
    const diis = toNumber(latestShare?.diis ?? row?.diis);
    const publicHolding = toNumber(latestShare?.public);
    const previousFiis = toNumber(previousShare?.fiis);
    const previousDiis = toNumber(previousShare?.diis);
    const companyAgeYears = row?.master_created_at ? (Date.now() - new Date(row.master_created_at).getTime()) / (365.25 * 24 * 60 * 60 * 1000) : null;
    const latestSales = toNumber(latestProfit?.sales ?? latestProfit?.revenue);
    const latestNetProfit = toNumber(latestProfit?.net_profit);
    const latestOpm = toNumber(latestProfit?.opm_percent);
    const previousOpm = toNumber(previousProfit?.opm_percent);

    const debtToEquity =
      borrowings !== null && (equityCapital !== null || reserves !== null)
        ? borrowings / Math.max((equityCapital || 0) + (reserves || 0), 1)
        : null;

    const currentOcf = toNumber(latestCash?.cash_from_operating_activity);
    const latestOcf = currentOcf;
    const last3Ocf = cashOcfHistory.slice(-3).map((item) => item.value).filter((v) => v !== null);
    const ocfPositiveLast3Years = last3Ocf.length === 3 && last3Ocf.every((value) => value > 0);
    const consistentProfitLast3Years = profitHistoryValues.slice(-3).every((item) => item.value !== null && item.value > 0);

    const promoterSeries = getRecentSeries(shareHistory, "promoters", 4);
    const fiiSeries = getRecentSeries(shareHistory, "fiis", 4);
    const diiSeries = getRecentSeries(shareHistory, "diis", 4);
    const publicSeries = getRecentSeries(shareHistory, "public", 4);

    const promoterTrendStats = getSeriesTrend(promoterSeries);
    const fiiTrendStats = getSeriesTrend(fiiSeries);
    const diiTrendStats = getSeriesTrend(diiSeries);
    const publicTrendStats = getSeriesTrend(publicSeries);

    const promoterHoldingTrend = classifyNetTrend(promoterTrendStats.netChange, 0.5);
    const promoterQuarterDrop = promoterTrendStats.maxQuarterDrop;
    const fiiHoldingTrend = classifyNetTrend(fiiTrendStats.netChange, 0.5);
    const diiHoldingTrend = classifyNetTrend(diiTrendStats.netChange, 0.5);
    const publicHoldingTrend = classifyPublicTrend(publicTrendStats.netChange);

    const promoterHoldingPass = promoters !== null && promoters > 35;
    const promoterSingleQuarterDropPass = promoterTrendStats.hasEnough && promoterQuarterDrop !== null && promoterQuarterDrop < 3;
    const promoterNet4QChangePass = promoterTrendStats.hasEnough && promoterTrendStats.netChange !== null && promoterTrendStats.netChange > -5;
    const industryKeyName = industryKey(row);
    const industryAvgPe = industryAvgPeMap[industryKeyName] ?? avg(overviewRows.map((item) => toNumber(item?.stock_pe)).filter((v) => v !== null));
    const peVsIndustry = peRatio !== null && industryAvgPe && industryAvgPe > 0 ? peRatio / industryAvgPe : null;
    const priceToBook = currentPrice !== null && bookValue !== null && bookValue > 0 ? currentPrice / bookValue : null;
    const priceToSales = marketCap !== null && latestSales !== null && latestSales > 0 ? marketCap / latestSales : null;
    const ebitda =
      toNumber(latestProfit?.operating_profit) !== null && toNumber(latestProfit?.depreciation) !== null
        ? toNumber(latestProfit?.operating_profit) + toNumber(latestProfit?.depreciation)
        : null;
    const evEbitda =
      marketCap !== null && borrowings !== null && cashEquivalents !== null && ebitda !== null && ebitda > 0
        ? (marketCap + borrowings - cashEquivalents) / ebitda
        : null;
    const interestCoverage = (() => {
      const operatingProfit = toNumber(latestProfit?.operating_profit);
      const interest = toNumber(latestProfit?.interest);
      if (operatingProfit === null || interest === null || interest <= 0) return null;
      return operatingProfit / interest;
    })();
    const debtorDays = toNumber(latestRatio?.debtor_days);
    const previousDebtorDays = toNumber(previousRatio?.debtor_days);
    const inventoryDays = toNumber(latestRatio?.inventory_days);
    const previousInventoryDays = toNumber(previousRatio?.inventory_days);
    const tier1Flags = {
      roce_gt_12: roce !== null && roce > 12,
      roe_gt_12: roe !== null && roe > 12,
      debt_to_equity_lt_15: debtToEquity !== null && debtToEquity < 1.5,
      ocf_positive_last_3_years: ocfPositiveLast3Years,
      promoter_holding_gt_35: promoterHoldingPass,
      promoter_single_quarter_drop_lt_3: promoterSingleQuarterDropPass,
      promoter_net_change_gt_minus_5: promoterNet4QChangePass,
      company_age_gt_5: companyAgeYears !== null && companyAgeYears > 5,
      consistent_profit_last_3_years: consistentProfitLast3Years,
    };
    const tier1Pass = Object.values(tier1Flags).every(Boolean);

    const peVsIndustryScore =
      peVsIndustry === null
        ? { score: 0, reason: "No industry P/E benchmark" }
        : peVsIndustry < 0.7
          ? { score: 15, reason: "P/E is well below industry average" }
          : peVsIndustry <= 0.9
            ? { score: 8, reason: "P/E is below industry average" }
            : { score: 0, reason: "P/E is not cheap vs industry" };
    const pbScore =
      priceToBook === null
        ? { score: 0, reason: "No P/B data" }
        : priceToBook < 1.5
          ? { score: 12, reason: "Price to book is attractive" }
          : priceToBook <= 3
            ? { score: 7, reason: "Price to book is acceptable" }
            : { score: 0, reason: "Price to book is expensive" };
    const evEbitdaScore =
      evEbitda === null
        ? { score: 0, reason: "No EV/EBITDA data" }
        : evEbitda < 10
          ? { score: 10, reason: "EV/EBITDA is attractive" }
          : evEbitda <= 15
            ? { score: 6, reason: "EV/EBITDA is acceptable" }
            : { score: 0, reason: "EV/EBITDA is expensive" };
    const dividendScore =
      dividendYield === null
        ? { score: 0, reason: "No dividend yield data" }
        : dividendYield > 2
          ? { score: 8, reason: "Dividend yield is attractive" }
          : dividendYield >= 1
            ? { score: 4, reason: "Dividend yield is moderate" }
            : { score: 0, reason: "Dividend yield is weak" };
    const priceToSalesScore =
      priceToSales === null
        ? { score: 0, reason: "No price-to-sales data" }
        : priceToSales < 1.5
          ? { score: 5, reason: "Price-to-sales is attractive" }
          : priceToSales <= 3
            ? { score: 3, reason: "Price-to-sales is acceptable" }
            : { score: 0, reason: "Price-to-sales is expensive" };
    const tier2Score = peVsIndustryScore.score + pbScore.score + evEbitdaScore.score + dividendScore.score + priceToSalesScore.score;

    const recentOpmValues = opmHistory.slice(-4).map((item) => item.value).filter((v) => v !== null);
    const opmStableOrExpanding =
      recentOpmValues.length >= 2 &&
      recentOpmValues[recentOpmValues.length - 1] >= recentOpmValues[0] &&
      recentOpmValues[recentOpmValues.length - 1] >= 10;
    const revenueGrowthScore =
      revenueCagr3y === null
        ? { score: 0, reason: "No revenue CAGR data" }
        : revenueCagr3y > 8
          ? { score: 7, reason: "Revenue growth is strong" }
          : revenueCagr3y >= 3
            ? { score: 4, reason: "Revenue growth is acceptable" }
            : { score: 0, reason: "Revenue growth is weak" };
    const opmScore =
      latestOpm === null
        ? { score: 0, reason: "No OPM data" }
        : latestOpm > 15 && opmStableOrExpanding
          ? { score: 8, reason: "OPM is strong and stable" }
          : latestOpm >= 10
            ? { score: 4, reason: "OPM is acceptable" }
            : { score: 0, reason: "OPM is weak" };
    const interestCoverageScore =
      interestCoverage === null
        ? { score: 0, reason: "No interest coverage data" }
        : interestCoverage > 5
          ? { score: 8, reason: "Interest coverage is strong" }
          : interestCoverage >= 3
            ? { score: 4, reason: "Interest coverage is acceptable" }
            : { score: 0, reason: "Interest coverage is weak" };
    const debtorDaysScore =
      debtorDays === null
        ? { score: 0, reason: "No debtor days data" }
        : debtorDays < 45
          ? { score: 7, reason: "Debtor days are efficient" }
          : debtorDays <= 90
            ? { score: 4, reason: "Debtor days are acceptable" }
            : { score: 0, reason: "Debtor days are stretched" };
    const tier3Score = revenueGrowthScore.score + opmScore.score + interestCoverageScore.score + debtorDaysScore.score;

    const promoterTrendScore =
      !promoterTrendStats.hasEnough || promoterTrendStats.netChange === null
        ? 0
        : promoterHoldingTrend === "increasing"
          ? 6
          : promoterHoldingTrend === "stable"
            ? 3
            : 0;
    const promoterDropScore =
      !promoterTrendStats.hasEnough || promoterQuarterDrop === null
        ? 0
        : promoterQuarterDrop <= 1
          ? 5
          : promoterQuarterDrop <= 2
            ? 3
            : 0;
    const fiiTrendScore =
      !fiiTrendStats.hasEnough || fiiTrendStats.netChange === null
        ? 0
        : fiiHoldingTrend === "increasing"
          ? 4
          : fiiHoldingTrend === "stable"
            ? 2
            : 0;
    const diiTrendScore =
      !diiTrendStats.hasEnough || diiTrendStats.netChange === null
        ? 0
        : diiHoldingTrend === "increasing"
          ? 3
          : diiHoldingTrend === "stable"
            ? 2
            : 0;
    const publicTrendScore =
      !publicTrendStats.hasEnough || publicTrendStats.netChange === null
        ? 0
        : publicTrendStats.netChange <= 0
          ? 2
          : publicTrendStats.netChange < 2
            ? 1
            : 0;
    const tier4Score = promoterTrendScore + promoterDropScore + fiiTrendScore + diiTrendScore + publicTrendScore;

    if (tier1Only && !tier1Pass) return null;

    const salesDeclineCount = countNegativeDeltas(revenueHistory.slice(-4).map((item) => item.value));
    const latestPayout = payoutHistory[payoutHistory.length - 1]?.value ?? null;
    const previousPayout = payoutHistory.length > 1 ? payoutHistory[payoutHistory.length - 2]?.value ?? null : null;
    const warningRows = [];
    const pushWarning = (code, severity, title, description) => warningRows.push({ code, severity, title, description });
    if (promoterQuarterDrop !== null && promoterQuarterDrop > 1.5) pushWarning("W1", "critical", "Promoter drop", "Promoter holding dropped sharply in a quarter.");
    if (fiiHoldingTrend === "decreasing" && diiHoldingTrend === "decreasing" && promoterHoldingTrend === "stable") pushWarning("W2", "critical", "Institutional exit", "FII and DII are both exiting while promoter is stable.");
    if (peVsIndustry !== null && peVsIndustry < 0.9 && salesDeclineCount >= 2) pushWarning("W3", "critical", "Value trap signal", "P/E is cheap but revenue is declining.");
    if (latestOcf !== null && latestNetProfit !== null && latestNetProfit > 0 && latestOcf < latestNetProfit * 0.5) pushWarning("W4", "moderate", "Earnings quality", "Cash flow is not converting well from profit.");
    if (borrowings !== null && previousBorrowings !== null && borrowings > previousBorrowings && (profitCagr3y === null || profitCagr3y < 5)) pushWarning("W5", "moderate", "Debt trap", "Debt is rising while profits are flat or weak.");
    if (latestPayout !== null && previousPayout !== null && latestPayout < previousPayout) pushWarning("W6", "moderate", "Dividend cut", "Dividend payout has reduced versus last year.");
    if (opmHistory.slice(-4).map((item) => item.value).filter((v) => v !== null).length >= 4) {
      const opmSeries = opmHistory.slice(-4).map((item) => item.value);
      if (opmSeries[3] < opmSeries[2] && opmSeries[2] < opmSeries[1]) pushWarning("W7", "moderate", "Margin erosion", "OPM has declined for 3 consecutive periods.");
    }
    if (promoterHoldingTrend === "decreasing" && fiiHoldingTrend === "decreasing") pushWarning("W8", "critical", "Everyone exiting", "Promoter and FII both show negative momentum.");
    if (debtorDays !== null && previousDebtorDays !== null && debtorDays - previousDebtorDays > 20) pushWarning("W9", "minor", "Debtor days up", "Collections are getting slower.");
    if (inventoryDays !== null && previousInventoryDays !== null && inventoryDays - previousInventoryDays > 30) pushWarning("W10", "minor", "Inventory buildup", "Inventory days have jumped sharply.");

    const penalty = warningRows.reduce((sum, warning) => {
      if (warning.severity === "critical") return sum + 5;
      if (warning.severity === "moderate") return sum + 3;
      return sum + 1;
    }, 0);
    const score = Math.max(0, Math.min(100, tier2Score + tier3Score + tier4Score - penalty));
    const grade = pickGrade(score);
    const recommendation = grade;
    const reasons = [
      tier1Pass ? "Tier 1 quality gates passed" : "Tier 1 quality gates failed",
      `Industry P/E benchmark: ${industryAvgPe ? industryAvgPe.toFixed(2) : "n/a"}`,
      `Warnings raised: ${warningRows.length}`,
    ];

    const rowResult = {
      ...row,
      latest_profit_period: latestProfit?.period_label || null,
      latest_profit_period_numeric: latestProfit?.period_numeric || null,
      latest_net_profit: latestProfit?.net_profit ?? null,
      latest_sales: latestSales ?? null,
      latest_eps: latestProfit?.eps ?? latestProfit?.net_profit_profit_for_eps ?? null,
      latest_opm: latestProfit?.opm_percent ?? null,
      latest_cash_period: latestCash?.period_label || null,
      latest_cash_period_numeric: latestCash?.period_numeric || null,
      cash_from_operating_activity: latestOcf,
      net_cash_flow: latestCash?.net_cash_flow ?? null,
      latest_balance_period: latestBalance?.period_label || null,
      latest_balance_period_numeric: latestBalance?.period_numeric || null,
      latest_borrowings: borrowings,
      latest_reserves: reserves,
      latest_equity_capital: equityCapital,
      latest_total_liabilities: totalLiabilities,
      latest_promoters: promoters,
      latest_fiis: fiis,
      latest_diis: diis,
      roe_percent: roe,
      roce_percent: roce,
      profit_loss_history: profitHistory,
      sales_history: revenueHistory,
      profit_history: profitHistoryValues,
      cash_flow_history: cashHistory,
      cash_history: cashOcfHistory,
      balance_history: balanceHistory,
      ratio_history: ratioHistory,
      shareholding_history: shareHistory,
      value_metrics: {
        company_age_years: companyAgeYears,
        industry_avg_pe: industryAvgPe,
        pe_vs_industry: peVsIndustry,
        price_to_book: priceToBook,
        price_to_sales: priceToSales,
        ev_ebitda: evEbitda,
        dividend_yield: dividendYield,
        roe,
        roce,
        debt_to_equity: debtToEquity,
        revenue_cagr_3y: revenueCagr3y,
        profit_cagr_3y: profitCagr3y,
        opm_percent: latestOpm,
        interest_coverage: interestCoverage,
        debtor_days: debtorDays,
        promoters,
        fiis,
        diis,
        public: publicHolding,
        promoter_net_change_4q: promoterTrendStats.netChange,
        promoter_max_quarter_drop_4q: promoterQuarterDrop,
        fii_net_change_4q: fiiTrendStats.netChange,
        dii_net_change_4q: diiTrendStats.netChange,
        public_net_change_4q: publicTrendStats.netChange,
        promoter_holding_trend: promoterHoldingTrend,
        fii_trend: fiiHoldingTrend,
        dii_trend: diiHoldingTrend,
        public_holding_trend: publicHoldingTrend,
      },
      analysis: {
        score,
        grade,
        recommendation,
        reasons: [
          `Tier 1: ${tier1Pass ? "passed" : "failed"}`,
          `Industry P/E: ${industryAvgPe ? industryAvgPe.toFixed(2) : "n/a"}`,
          `Warnings: ${warningRows.length}`,
        ],
        metrics: {
          company_age_years: companyAgeYears,
          industry_avg_pe: industryAvgPe,
          pe_vs_industry: peVsIndustry,
          price_to_book: priceToBook,
          price_to_sales: priceToSales,
          dividend_yield: dividendYield,
          revenue_cagr_3y: revenueCagr3y,
          profit_cagr_3y: profitCagr3y,
          eps_cagr_3y: epsCagr3y,
          opm_percent: latestOpm,
          roe,
          roce,
          debt_to_equity: debtToEquity,
          pe_ratio: peRatio,
          ev_ebitda: evEbitda,
          interest_coverage: interestCoverage,
          promoters,
          fiis,
          diis,
          public: publicHolding,
          promoter_net_change_4q: promoterTrendStats.netChange,
          promoter_max_quarter_drop_4q: promoterQuarterDrop,
          fii_net_change_4q: fiiTrendStats.netChange,
          dii_net_change_4q: diiTrendStats.netChange,
          public_net_change_4q: publicTrendStats.netChange,
          debtor_days: debtorDays,
        },
        flags: {
          tier1_passed: tier1Pass,
          roce_gt_12: tier1Flags.roce_gt_12,
          roe_gt_12: tier1Flags.roe_gt_12,
          debt_to_equity_lt_15: tier1Flags.debt_to_equity_lt_15,
          ocf_positive_last_3_years: tier1Flags.ocf_positive_last_3_years,
          promoter_holding_gt_35: tier1Flags.promoter_holding_gt_35,
          promoter_single_quarter_drop_lt_3: tier1Flags.promoter_single_quarter_drop_lt_3,
          promoter_net_change_gt_minus_5: tier1Flags.promoter_net_change_gt_minus_5,
          company_age_gt_5: tier1Flags.company_age_gt_5,
          consistent_profit_last_3_years: tier1Flags.consistent_profit_last_3_years,
          pe_vs_industry_full: peVsIndustry !== null && peVsIndustry < 0.7,
          pe_vs_industry_partial: peVsIndustry !== null && peVsIndustry >= 0.7 && peVsIndustry <= 0.9,
          pb_full: priceToBook !== null && priceToBook < 1.5,
          pb_partial: priceToBook !== null && priceToBook >= 1.5 && priceToBook <= 3,
          ev_ebitda_full: evEbitda !== null && evEbitda < 10,
          ev_ebitda_partial: evEbitda !== null && evEbitda >= 10 && evEbitda <= 15,
          dividend_full: dividendYield !== null && dividendYield > 2,
          dividend_partial: dividendYield !== null && dividendYield >= 1 && dividendYield <= 2,
          price_to_sales_full: priceToSales !== null && priceToSales < 1.5,
          price_to_sales_partial: priceToSales !== null && priceToSales >= 1.5 && priceToSales <= 3,
          opm_full: latestOpm !== null && latestOpm > 15 && opmStableOrExpanding,
          opm_partial: latestOpm !== null && latestOpm >= 10,
          revenue_growth_strong: revenueCagr3y !== null && revenueCagr3y > 8,
          revenue_growth_partial: revenueCagr3y !== null && revenueCagr3y >= 3 && revenueCagr3y <= 8,
          interest_coverage_full: interestCoverage !== null && interestCoverage > 5,
          interest_coverage_partial: interestCoverage !== null && interestCoverage >= 3 && interestCoverage <= 5,
          debtor_days_full: debtorDays !== null && debtorDays < 45,
          debtor_days_partial: debtorDays !== null && debtorDays >= 45 && debtorDays <= 90,
          promoter_trend_increasing: promoterHoldingTrend === "increasing",
          promoter_trend_stable: promoterHoldingTrend === "stable",
          promoter_drop_full: promoterQuarterDrop !== null && promoterQuarterDrop <= 1,
          promoter_drop_partial: promoterQuarterDrop !== null && promoterQuarterDrop > 1 && promoterQuarterDrop <= 2,
          fii_trend_increasing: fiiHoldingTrend === "increasing",
          fii_trend_stable: fiiHoldingTrend === "stable",
          dii_trend_increasing: diiHoldingTrend === "increasing",
          dii_trend_stable: diiHoldingTrend === "stable",
          public_trend_stable_or_decreasing: publicTrendStats.netChange !== null && publicTrendStats.netChange <= 0,
          public_trend_slight_increase: publicTrendStats.netChange !== null && publicTrendStats.netChange > 0 && publicTrendStats.netChange < 2,
        },
        warnings: warningRows,
        tier_scores: {
          tier1: 0,
          tier2: tier2Score,
          tier3: tier3Score,
          tier4: tier4Score,
        },
      },
    };

    return rowResult;
  });
  } catch (error) {
    console.error("[VALUE] buildValueAnalysisRows failed", error);
    throw error;
  }
};

const getValueAnalysisRows = async ({ limit = 20, grade = "ALL", minScore = null } = {}, db = pool) => {
  const rows = (await buildValueAnalysisRows({}, db)).filter(Boolean).filter((row) => matchesGradeFilter(row, grade) && matchesScoreFilter(row, minScore));
  return sortByOverallRank(rows).slice(0, Number(limit) || 20);
};

const getValueAnalysisBySymbol = async (symbol, db = pool) => {
  const rows = (await buildValueAnalysisRows({}, db)).filter(Boolean);
  return rows.find((row) => String(row.symbol || "").toUpperCase() === String(symbol || "").trim().toUpperCase()) || null;
};

const getValueAnalysisBuckets = async ({ limit = 50, grade = "ALL", minScore = null } = {}, db = pool) => {
  const rows = (await buildValueAnalysisRows({}, db)).filter(Boolean).filter((row) => matchesGradeFilter(row, grade) && matchesScoreFilter(row, minScore));
  const overallRows = sortByOverallRank(rows).slice(0, Number(limit) || 50);
  const tier1Rows = sortByOverallRank(rows.filter((row) => Boolean(row?.analysis?.flags?.tier1_passed))).slice(0, Number(limit) || 50);
  const tier2Rows = sortByTierScore(rows.filter((row) => Number(row?.analysis?.tier_scores?.tier2 || 0) > 0), "tier2").slice(0, Number(limit) || 50);
  const tier3Rows = sortByTierScore(rows.filter((row) => Number(row?.analysis?.tier_scores?.tier3 || 0) > 0), "tier3").slice(0, Number(limit) || 50);
  const tier4Rows = sortByTierScore(rows.filter((row) => Number(row?.analysis?.tier_scores?.tier4 || 0) > 0), "tier4").slice(0, Number(limit) || 50);

  return {
    total: rows.length,
    overallRows,
    tierRows: {
      tier1: tier1Rows,
      tier2: tier2Rows,
      tier3: tier3Rows,
      tier4: tier4Rows,
    },
  };
};

module.exports = {
  buildValueAnalysisRows,
  getValueAnalysisRows,
  getValueAnalysisBySymbol,
  getValueAnalysisBuckets,
};
