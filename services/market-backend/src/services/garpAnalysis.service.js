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
  if (score >= 85) return "Strong Buy";
  if (score >= 70) return "Buy";
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
    const salesDiff = Number(b?.garp_metrics?.revenue_cagr_3y || 0) - Number(a?.garp_metrics?.revenue_cagr_3y || 0);
    if (salesDiff !== 0) return salesDiff;
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

const buildGarpAnalysisRows = async (db = pool) => {
  try {
  if (!(await tableExists("stock_fundamental_overview", db))) return [];

  const overviewRes = await db.query(
    `
      SELECT
        sm.id AS master_id,
        sm.symbol,
        sm.name,
        sm.exchange,
        sm.screener_status,
        o.active_stock_id,
        o.company_name,
        o.market_cap,
        o.current_price,
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
      ORDER BY COALESCE(o.roce, o.roe, 0) DESC, COALESCE(o.dividend_yield, 0) DESC, sm.symbol ASC
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
      ? fetchLatestRowsByMasterIds(
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

  return overviewRows.map((row) => {
    const masterId = String(row.master_id);
    const ratioHistory = sortByPeriodNumericAsc(ratiosByMaster[masterId] || []);
    const profitHistory = sortByPeriodNumericAsc(profitByMaster[masterId] || []);
    const cashHistory = sortByPeriodNumericAsc(cashByMaster[masterId] || []);
    const shareHistory = sortByPeriodNumericAsc(shareByMaster[masterId] || []);
    const latestBalance = (balanceByMaster[masterId] || [])[0] || null;

    const latestRatio = ratioHistory[ratioHistory.length - 1] || null;
    const latestProfit = profitHistory[profitHistory.length - 1] || null;
    const latestCash = cashHistory[cashHistory.length - 1] || null;
    const previousCash = cashHistory.length > 1 ? cashHistory[cashHistory.length - 2] || null : null;
    const latestShare = shareHistory[shareHistory.length - 1] || null;
    const previousShare = shareHistory.length > 1 ? shareHistory[shareHistory.length - 2] || null : null;

    const revenueHistory = buildHistory(profitHistory, ["revenue", "sales"]);
    const profitHistoryValues = buildHistory(profitHistory, ["net_profit"]);
    const epsHistory = buildHistory(profitHistory, ["eps", "net_profit_profit_for_eps"]);
    const opmHistory = buildHistory(profitHistory, ["opm_percent"]);
    const cashOcfHistory = buildHistory(cashHistory, ["cash_from_operating_activity"]);
    const inventoryDaysHistory = buildHistory(ratioHistory, ["inventory_days"]);
    const workingCapDaysHistory = buildHistory(ratioHistory, ["working_capital_days"]);

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

    const latestOpm = toNumber(latestProfit?.opm_percent);
    const recentOpmValues = lastN(opmHistory, 4).map((item) => item.value).filter((v) => v !== null);
    const opmPass =
      latestOpm !== null &&
      latestOpm > 10 &&
      recentOpmValues.length >= 2 &&
      trendIsStableOrIncreasing(recentOpmValues) &&
      avg(recentOpmValues) !== null &&
      avg(recentOpmValues) >= 10;

    const roe = toNumber(latestRatio?.roe_percent ?? row?.roe);
    const roce = toNumber(latestRatio?.roce_percent ?? row?.roce);
    const borrowings = toNumber(latestBalance?.borrowings ?? latestBalance?.borrowing);
    const reserves = toNumber(latestBalance?.reserves);
    const equityCapital = toNumber(latestBalance?.equity_capital);
    const cashEquivalents = toNumber(latestBalance?.cash_equivalents);
    const totalLiabilities = toNumber(latestBalance?.total_liabilities);
    const marketCap = toNumber(row?.market_cap);
    const currentPrice = toNumber(row?.current_price);
    const bookValue = toNumber(row?.book_value);
    const peRatio = toNumber(row?.stock_pe);
    const promoters = toNumber(latestShare?.promoters);
    const fiis = toNumber(latestShare?.fiis);
    const diis = toNumber(latestShare?.diis);
    const previousFiis = toNumber(previousShare?.fiis);
    const previousDiis = toNumber(previousShare?.diis);

    const debtToEquity =
      borrowings !== null && (equityCapital !== null || reserves !== null)
        ? borrowings / Math.max((equityCapital || 0) + (reserves || 0), 1)
        : null;

    const currentOcf = toNumber(latestCash?.cash_from_operating_activity);
    const last3Ocf = cashOcfHistory.slice(-3).map((item) => item.value).filter((v) => v !== null);
    const ocfPositiveLast3Years = last3Ocf.length === 3 && last3Ocf.every((value) => value > 0);

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

    const promoterHoldingPass = promoters !== null && promoters > 40;
    const promoterSingleQuarterDropPass = promoterTrendStats.hasEnough && promoterQuarterDrop !== null && promoterQuarterDrop < 3;
    const promoterNet4QChangePass = promoterTrendStats.hasEnough && promoterTrendStats.netChange !== null && promoterTrendStats.netChange > -5;
    const tier1Flags = {
      roce_gt_15: roce !== null && roce > 15,
      roe_gt_15: roe !== null && roe > 15,
      debt_to_equity_lt_1: debtToEquity !== null && debtToEquity < 1,
      ocf_positive_last_3_years: ocfPositiveLast3Years,
      promoter_holding_gt_40: promoterHoldingPass,
      promoter_single_quarter_drop_lt_3: promoterSingleQuarterDropPass,
      promoter_net_change_gt_minus_5: promoterNet4QChangePass,
    };
    const tier1Pass = Object.values(tier1Flags).every(Boolean);

    const revenueScore =
      revenueCagr3y === null
        ? { score: 0, reason: "Revenue CAGR unavailable" }
        : revenueCagr3y > 15
          ? { score: 10, reason: "Revenue CAGR is strong" }
          : revenueCagr3y >= 10
            ? { score: 5, reason: "Revenue CAGR is acceptable" }
            : { score: 0, reason: "Revenue CAGR is weak" };
    const profitScore =
      profitCagr3y === null
        ? { score: 0, reason: "Profit CAGR unavailable" }
        : profitCagr3y > 20
          ? { score: 10, reason: "Profit CAGR is strong" }
          : profitCagr3y >= 15
            ? { score: 5, reason: "Profit CAGR is acceptable" }
            : { score: 0, reason: "Profit CAGR is weak" };
    const latestOpmValue = toNumber(latestProfit?.opm_percent);
    const previousOpmValue = opmHistory.length > 1 ? toNumber(opmHistory[opmHistory.length - 2]?.value) : null;
    const opmExpanding = latestOpmValue !== null && previousOpmValue !== null && latestOpmValue > previousOpmValue;
    const opmScore =
      latestOpmValue === null
        ? { score: 0, reason: "OPM unavailable" }
        : latestOpmValue <= 10
          ? { score: 0, reason: "OPM is below 10%" }
          : opmExpanding
            ? { score: 10, reason: "OPM is above 10% and expanding" }
            : { score: 5, reason: "OPM is above 10% but stable" };
    const epsScore =
      epsCagr3y === null
        ? { score: 0, reason: "EPS growth unavailable" }
        : epsCagr3y > 15
          ? { score: 10, reason: "EPS growth is strong" }
          : epsCagr3y >= 10
            ? { score: 5, reason: "EPS growth is acceptable" }
            : { score: 0, reason: "EPS growth is weak" };

    const tier2Score = revenueScore.score + profitScore.score + opmScore.score + epsScore.score;

    const pegRatio = (() => {
      if (peRatio === null || profitCagr3y === null || profitCagr3y <= 0) return null;
      return peRatio / profitCagr3y;
    })();
    const priceToBook = (() => {
      if (currentPrice === null || bookValue === null || bookValue <= 0) return null;
      return currentPrice / bookValue;
    })();
    const ebitda = (() => {
      const operatingProfit = toNumber(latestProfit?.operating_profit);
      const depreciation = toNumber(latestProfit?.depreciation);
      if (operatingProfit === null || depreciation === null) return null;
      return operatingProfit + depreciation;
    })();
    const evEbitda = (() => {
      if (marketCap === null || borrowings === null || cashEquivalents === null || ebitda === null || ebitda <= 0) {
        return null;
      }
      return (marketCap + borrowings - cashEquivalents) / ebitda;
    })();
    const interestCoverage = (() => {
      const operatingProfit = toNumber(latestProfit?.operating_profit);
      const interest = toNumber(latestProfit?.interest);
      if (operatingProfit === null || interest === null || interest <= 0) return null;
      return operatingProfit / interest;
    })();

    const pegScore =
      pegRatio === null
        ? { score: 0, reason: "PEG unavailable" }
        : pegRatio < 1.5
          ? { score: 15, reason: "PEG is attractive" }
          : pegRatio <= 2.0
            ? { score: 8, reason: "PEG is acceptable" }
            : { score: 0, reason: "PEG is expensive" };
    const peScore =
      peRatio === null
        ? { score: 0, reason: "P/E unavailable" }
        : peRatio < 50
          ? { score: 10, reason: "P/E is attractive" }
          : peRatio <= 70
            ? { score: 5, reason: "P/E is acceptable" }
            : { score: 0, reason: "P/E is expensive" };
    const pbScore =
      priceToBook === null
        ? { score: 0, reason: "P/B unavailable" }
        : priceToBook < 10
          ? { score: 5, reason: "P/B is attractive" }
          : priceToBook <= 15
            ? { score: 3, reason: "P/B is acceptable" }
            : { score: 0, reason: "P/B is expensive" };
    const evEbitdaScore =
      evEbitda === null
        ? { score: 0, reason: "EV/EBITDA unavailable" }
        : evEbitda < 30
          ? { score: 5, reason: "EV/EBITDA is attractive" }
          : evEbitda <= 40
            ? { score: 3, reason: "EV/EBITDA is acceptable" }
            : { score: 0, reason: "EV/EBITDA is expensive" };
    const tier3Score = pegScore.score + peScore.score + pbScore.score + evEbitdaScore.score;

    const promoterTrendScore =
      !promoterTrendStats.hasEnough || promoterTrendStats.netChange === null
        ? 0
        : promoterHoldingTrend === "increasing"
          ? 8
          : promoterHoldingTrend === "stable"
            ? 4
            : 0;
    const promoterDropScore =
      !promoterTrendStats.hasEnough || promoterQuarterDrop === null
        ? 0
        : promoterQuarterDrop <= 1
          ? 7
          : promoterQuarterDrop <= 2
            ? 4
            : 0;
    const fiiTrendScore =
      !fiiTrendStats.hasEnough || fiiTrendStats.netChange === null
        ? 0
        : fiiHoldingTrend === "increasing"
          ? 5
          : fiiHoldingTrend === "stable"
            ? 3
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

    const score = tier1Pass ? tier2Score + tier3Score + tier4Score : 0;

    const reasons = [
      tier1Pass ? "Tier 1 quality gates passed" : "Tier 1 quality gates failed",
      revenueScore.reason,
      profitScore.reason,
      opmScore.reason,
      epsScore.reason,
      pegScore.reason,
      peScore.reason,
      pbScore.reason,
      evEbitdaScore.reason,
      `Promoter trend: ${promoterHoldingTrend}`,
      promoterQuarterDrop === null
        ? "Promoter quarter drop unavailable"
        : `Promoter max quarter drop is ${promoterQuarterDrop.toFixed(2)}pp`,
      `FII trend: ${fiiHoldingTrend}`,
      `DII trend: ${diiHoldingTrend}`,
      `Public holding trend: ${publicHoldingTrend}`,
    ];

    const recommendation = !tier1Pass
      ? "Reject"
      : score >= 85
        ? "Strong Buy"
        : score >= 70
          ? "Buy"
          : score >= 55
            ? "Watch"
            : "Reject";

    if (!tier1Pass) return null;

    const rowResult = {
      ...row,
      latest_profit_period: latestProfit?.period_label || null,
      latest_profit_period_numeric: latestProfit?.period_numeric || null,
      latest_net_profit: latestProfit?.net_profit ?? null,
      latest_sales: latestProfit?.sales ?? latestProfit?.revenue ?? null,
      latest_eps: latestProfit?.eps ?? latestProfit?.net_profit_profit_for_eps ?? null,
      latest_opm: latestProfit?.opm_percent ?? null,
      latest_cash_period: latestCash?.period_label || null,
      latest_cash_period_numeric: latestCash?.period_numeric || null,
      cash_from_operating_activity: currentOcf,
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
      sales_history: revenueHistory,
      profit_history: profitHistoryValues,
      cash_history: cashOcfHistory,
      ratio_history: ratioHistory,
      shareholding_history: shareHistory,
      garp_metrics: {
        revenue_cagr_3y: revenueCagr3y,
        profit_cagr_3y: profitCagr3y,
        eps_cagr_3y: epsCagr3y,
        opm_percent: latestOpm,
        roe,
        roce,
        debt_to_equity: debtToEquity,
        pe_ratio: peRatio,
        price_to_book: priceToBook,
        peg_ratio: pegRatio,
        ev_ebitda: evEbitda,
        interest_coverage: interestCoverage,
        promoters,
        fiis,
        diis,
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
        grade: pickGrade(score),
        recommendation,
        reasons,
        metrics: {
          revenue_cagr_3y: revenueCagr3y,
          profit_cagr_3y: profitCagr3y,
          eps_cagr_3y: epsCagr3y,
          opm_percent: latestOpm,
          roe,
          roce,
          debt_to_equity: debtToEquity,
          pe_ratio: peRatio,
          price_to_book: priceToBook,
          peg_ratio: pegRatio,
          ev_ebitda: evEbitda,
          interest_coverage: interestCoverage,
          promoters,
          fiis,
          diis,
          promoter_net_change_4q: promoterTrendStats.netChange,
          promoter_max_quarter_drop_4q: promoterQuarterDrop,
          fii_net_change_4q: fiiTrendStats.netChange,
          dii_net_change_4q: diiTrendStats.netChange,
          public_net_change_4q: publicTrendStats.netChange,
        },
        flags: {
          tier1_passed: tier1Pass,
          roce_gt_15: tier1Flags.roce_gt_15,
          roe_gt_15: tier1Flags.roe_gt_15,
          debt_to_equity_lt_1: tier1Flags.debt_to_equity_lt_1,
          ocf_positive_last_3_years: tier1Flags.ocf_positive_last_3_years,
          promoter_holding_gt_40: tier1Flags.promoter_holding_gt_40,
          promoter_single_quarter_drop_lt_3: tier1Flags.promoter_single_quarter_drop_lt_3,
          promoter_net_change_gt_minus_5: tier1Flags.promoter_net_change_gt_minus_5,
          revenue_growth_strong: revenueCagr3y !== null && revenueCagr3y > 15,
          profit_growth_strong: profitCagr3y !== null && profitCagr3y > 20,
          opm_full: latestOpmValue !== null && latestOpmValue > 10 && opmExpanding,
          opm_partial: latestOpmValue !== null && latestOpmValue > 10 && !opmExpanding,
          eps_growth_strong: epsCagr3y !== null && epsCagr3y > 15,
          peg_full: pegRatio !== null && pegRatio < 1.5,
          peg_partial: pegRatio !== null && pegRatio >= 1.5 && pegRatio <= 2.0,
          pe_full: peRatio !== null && peRatio < 50,
          pe_partial: peRatio !== null && peRatio >= 50 && peRatio <= 70,
          pb_full: priceToBook !== null && priceToBook < 10,
          pb_partial: priceToBook !== null && priceToBook >= 10 && priceToBook <= 15,
          ev_ebitda_full: evEbitda !== null && evEbitda < 30,
          ev_ebitda_partial: evEbitda !== null && evEbitda >= 30 && evEbitda <= 40,
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
    console.error("[GARP] buildGarpAnalysisRows failed", error);
    throw error;
  }
};

const getGarpAnalysisRows = async ({ limit = 20, grade = "ALL", minScore = null } = {}, db = pool) => {
  const rows = (await buildGarpAnalysisRows(db)).filter(Boolean).filter((row) => matchesGradeFilter(row, grade) && matchesScoreFilter(row, minScore));
  return sortByOverallRank(rows).slice(0, Number(limit) || 20);
};

const getGarpAnalysisBySymbol = async (symbol, db = pool) => {
  const rows = (await buildGarpAnalysisRows(db)).filter(Boolean);
  return rows.find((row) => String(row.symbol || "").toUpperCase() === String(symbol || "").trim().toUpperCase()) || null;
};

const getGarpAnalysisBuckets = async ({ limit = 50, grade = "ALL", minScore = null } = {}, db = pool) => {
  const rows = (await buildGarpAnalysisRows(db)).filter(Boolean).filter((row) => matchesGradeFilter(row, grade) && matchesScoreFilter(row, minScore));
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
  getGarpAnalysisRows,
  getGarpAnalysisBySymbol,
  getGarpAnalysisBuckets,
};
