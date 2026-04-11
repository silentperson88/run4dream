const { pool } = require("../config/db");

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const safeJsonArray = (value) => (Array.isArray(value) ? value : []);

const parsePeriodParts = (periodNumeric) => {
  const text = String(periodNumeric || "").trim();
  const match = text.match(/^(\d{2})-(\d{4})$/);
  if (!match) return { month: null, year: null };
  return { month: Number(match[1]), year: Number(match[2]) };
};

const sortByPeriodNumericDesc = (rows = []) =>
  [...rows].sort((a, b) => {
    const aParts = parsePeriodParts(a?.period_numeric);
    const bParts = parsePeriodParts(b?.period_numeric);
    if (aParts.year !== bParts.year) return (bParts.year || 0) - (aParts.year || 0);
    if (aParts.month !== bParts.month) return (bParts.month || 0) - (aParts.month || 0);
    return 0;
  });

const pickGrade = (score) => {
  if (score >= 80) return "A";
  if (score >= 65) return "B";
  if (score >= 50) return "C";
  return "D";
};

const scoreYield = (yieldPct) => {
  if (yieldPct === null) return { score: 0, reason: "No dividend yield data" };
  if (yieldPct >= 6) return { score: 22, reason: "Very strong yield" };
  if (yieldPct >= 4) return { score: 18, reason: "Strong yield" };
  if (yieldPct >= 2) return { score: 14, reason: "Healthy yield" };
  if (yieldPct > 0) return { score: 8, reason: "Some dividend yield" };
  return { score: 0, reason: "No meaningful yield" };
};

const scoreROE = (roe) => {
  if (roe === null) return { score: 0, reason: "No ROE data" };
  if (roe >= 20) return { score: 14, reason: "Excellent ROE" };
  if (roe >= 15) return { score: 12, reason: "Strong ROE" };
  if (roe >= 10) return { score: 8, reason: "Decent ROE" };
  if (roe >= 5) return { score: 4, reason: "Weak ROE" };
  return { score: 0, reason: "Poor ROE" };
};

const scoreROCE = (roce) => {
  if (roce === null) return { score: 0, reason: "No ROCE data" };
  if (roce >= 20) return { score: 16, reason: "Excellent ROCE" };
  if (roce >= 15) return { score: 13, reason: "Strong ROCE" };
  if (roce >= 10) return { score: 8, reason: "Decent ROCE" };
  if (roce >= 5) return { score: 4, reason: "Weak ROCE" };
  return { score: 0, reason: "Poor ROCE" };
};

const scoreConsistency = (positivePeriods, totalPeriods, label) => {
  if (!totalPeriods) return { score: 0, reason: `No ${label} history` };
  const ratio = positivePeriods / totalPeriods;
  if (ratio >= 0.9) return { score: 18, reason: `Very consistent ${label}` };
  if (ratio >= 0.75) return { score: 14, reason: `Good ${label} consistency` };
  if (ratio >= 0.6) return { score: 8, reason: `Mixed ${label} consistency` };
  return { score: 0, reason: `Weak ${label} consistency` };
};

const scorePayout = (payout) => {
  if (payout === null) return { score: 0, reason: "No payout ratio data" };
  if (payout >= 20 && payout <= 70) return { score: 12, reason: "Healthy payout ratio" };
  if (payout > 70 && payout <= 100) return { score: 6, reason: "High payout ratio" };
  if (payout > 0 && payout < 20) return { score: 4, reason: "Low payout ratio" };
  if (payout > 100) return { score: 2, reason: "Very high payout ratio" };
  return { score: 0, reason: "No payout ratio support" };
};

const scoreDebt = (borrowings, reserves, totalLiabilities) => {
  if (borrowings === null && reserves === null && totalLiabilities === null) {
    return { score: 0, reason: "No balance-sheet leverage data" };
  }

  const debt = borrowings ?? 0;
  const reserve = reserves ?? 0;
  const liabilities = totalLiabilities ?? 0;

  if (debt === 0) return { score: 10, reason: "Debt-light balance sheet" };
  if (reserve > 0 && debt <= reserve * 0.5) return { score: 8, reason: "Comfortable debt vs reserves" };
  if (reserve > 0 && debt <= reserve) return { score: 5, reason: "Moderate debt vs reserves" };
  if (liabilities > 0 && debt <= liabilities * 0.25) return { score: 4, reason: "Debt contained" };
  return { score: 0, reason: "Debt looks heavy" };
};

const scoreCashFlow = (positivePeriods, totalPeriods) => scoreConsistency(positivePeriods, totalPeriods, "cash flow");

const calculateCagr = (startValue, endValue, years) => {
  const start = toNumber(startValue);
  const end = toNumber(endValue);
  if (!start || !end || start <= 0 || end <= 0 || !years || years <= 0) return null;
  return (Math.pow(end / start, 1 / years) - 1) * 100;
};

const formatPercent = (value) => {
  const parsed = toNumber(value);
  return parsed === null ? "n/a" : `${parsed.toFixed(2)}%`;
};

const buildYearlyHistory = (rows = [], key) =>
  sortByPeriodNumericDesc(rows)
    .map((row) => ({
      period_label: row?.period_label || row?.period || null,
      period_numeric: row?.period_numeric || null,
      period_index: row?.period_index ?? null,
      value: toNumber(row?.[key]),
      payout_ratio: toNumber(String(row?.dividend_payout_percent || "").replace(/[^0-9.\-]+/g, "")),
    }))
    .filter((row) => row.period_numeric);

const groupRowsByMasterId = (rows = []) => {
  return rows.reduce((acc, row) => {
    const key = String(row?.master_id || "");
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});
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
        updated_at DESC,
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
        END DESC NULLS LAST,
        CASE
          WHEN period_numeric ~ '^[0-9]{2}-[0-9]{4}$' THEN split_part(period_numeric, '-', 1)::int
          ELSE NULL
        END DESC NULLS LAST,
        updated_at DESC,
        id DESC
    `,
    [masterIds.map(Number)],
  );
  return rows;
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

const buildDividendAnalysisRows = async ({ limit = 50 } = {}, db = pool) => {
  const overviewTableExists = await tableExists("stock_fundamental_overview", db);
  if (!overviewTableExists) return [];

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
      ORDER BY COALESCE(o.dividend_yield, 0) DESC, COALESCE(o.roce, o.roe, 0) DESC, sm.symbol ASC
    `,
  );

  const overviewRows = overviewRes.rows || [];
  const masterIds = overviewRows.map((row) => Number(row.master_id)).filter((value) => Number.isFinite(value));
  if (!masterIds.length) return [];

  const [ratiosExists, profitExists, cashExists, balanceExists] = await Promise.all([
    tableExists("stock_fundamental_ratios_periods", db),
    tableExists("stock_fundamental_profit_loss_periods", db),
    tableExists("stock_fundamental_cash_flow_periods", db),
    tableExists("stock_fundamental_balance_sheet_periods", db),
  ]);

  const [ratiosRows, profitRows, cashRows, balanceRows] = await Promise.all([
    ratiosExists
      ? fetchLatestRowsByMasterIds(
          "stock_fundamental_ratios_periods",
          masterIds,
          "master_id, debtor_days, inventory_days, days_payable, cash_conversion_cycle, working_capital_days, roce_percent, roe_percent, last_updated_at",
          db,
        )
      : [],
    profitExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_profit_loss_periods",
          masterIds,
          "master_id, period_label, period_numeric, net_profit, operating_profit, sales, dividend_payout_percent, last_updated_at, id, updated_at",
          db,
        )
      : [],
    cashExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_cash_flow_periods",
          masterIds,
          "master_id, period_label, period_numeric, cash_from_operating_activity, cash_from_investing_activity, cash_from_financing_activity, net_cash_flow, last_updated_at, id, updated_at",
          db,
        )
      : [],
    balanceExists
      ? fetchLatestRowsByMasterIds(
          "stock_fundamental_balance_sheet_periods",
          masterIds,
          "master_id, period_label, period_numeric, borrowing, borrowings, reserves, equity_capital, total_liabilities, total_assets, last_updated_at",
          db,
        )
      : [],
  ]);

  const ratiosByMaster = groupRowsByMasterId(ratiosRows);
  const profitByMaster = groupRowsByMasterId(profitRows);
  const cashByMaster = groupRowsByMasterId(cashRows);
  const balanceByMaster = groupRowsByMasterId(balanceRows);

  return overviewRows.map((row) => {
    const masterId = String(row.master_id);
    const profitHistory = profitByMaster[masterId] || [];
    const cashHistory = cashByMaster[masterId] || [];
    const latestProfit = profitHistory[0] || null;
    const latestCash = cashHistory[0] || null;
    const latestRatio = (ratiosByMaster[masterId] || [])[0] || null;
    const latestBalance = (balanceByMaster[masterId] || [])[0] || null;

    const positiveProfitPeriods = profitHistory.filter((item) => toNumber(item?.net_profit) > 0).length;
    const totalProfitPeriods = profitHistory.length;
    const positiveCashPeriods = cashHistory.filter((item) => toNumber(item?.net_cash_flow) > 0).length;
    const totalCashPeriods = cashHistory.length;

    return {
      ...row,
      latest_profit_period: latestProfit?.period_label || null,
      latest_profit_period_numeric: latestProfit?.period_numeric || null,
      latest_net_profit: latestProfit?.net_profit ?? null,
      latest_operating_profit: latestProfit?.operating_profit ?? null,
      latest_sales: latestProfit?.sales ?? null,
      dividend_payout_percent: latestProfit?.dividend_payout_percent ?? null,
      latest_profit_last_updated_at: latestProfit?.last_updated_at || null,
      latest_cash_period: latestCash?.period_label || null,
      latest_cash_period_numeric: latestCash?.period_numeric || null,
      cash_from_operating_activity: latestCash?.cash_from_operating_activity ?? null,
      net_cash_flow: latestCash?.net_cash_flow ?? null,
      cash_last_updated_at: latestCash?.last_updated_at || null,
      latest_balance_period: latestBalance?.period_label || null,
      latest_balance_period_numeric: latestBalance?.period_numeric || null,
      latest_borrowings: toNumber(latestBalance?.borrowings ?? latestBalance?.borrowing),
      latest_reserves: toNumber(latestBalance?.reserves),
      latest_equity_capital: toNumber(latestBalance?.equity_capital),
      latest_total_liabilities: toNumber(latestBalance?.total_liabilities),
      latest_total_assets: toNumber(latestBalance?.total_assets),
      balance_last_updated_at: latestBalance?.last_updated_at || null,
      debtor_days: latestRatio?.debtor_days ?? null,
      inventory_days: latestRatio?.inventory_days ?? null,
      days_payable: latestRatio?.days_payable ?? null,
      cash_conversion_cycle: latestRatio?.cash_conversion_cycle ?? null,
      working_capital_days: latestRatio?.working_capital_days ?? null,
      roce_percent: latestRatio?.roce_percent ?? null,
      roe_percent: latestRatio?.roe_percent ?? null,
      ratios_last_updated_at: latestRatio?.last_updated_at || null,
      profit_rows: profitHistory,
      positive_profit_periods: positiveProfitPeriods,
      total_profit_periods: totalProfitPeriods,
      cash_rows: cashHistory,
      positive_cash_periods: positiveCashPeriods,
      total_cash_periods: totalCashPeriods,
    };
  });
};

const getDividendAnalysisRows = async ({ limit = 50 } = {}, db = pool) => buildDividendAnalysisRows({ limit }, db);

const getDividendAnalysisBySymbol = async (symbol, db = pool) => {
  const rows = await buildDividendAnalysisRows({ limit: 5000 }, db);
  return rows.find((row) => String(row.symbol || "").toUpperCase() === String(symbol || "").trim().toUpperCase()) || null;
};

const scoreDividendCandidate = (row) => {
  const dividendYield = toNumber(row?.dividend_yield);
  const roe = toNumber(row?.roe_percent ?? row?.roe);
  const roce = toNumber(row?.roce_percent ?? row?.roce);
  const borrowings = toNumber(row?.latest_borrowings);
  const reserves = toNumber(row?.latest_reserves);
  const equityCapital = toNumber(row?.latest_equity_capital);
  const totalLiabilities = toNumber(row?.latest_total_liabilities);
  const dividendHistory = buildYearlyHistory(safeJsonArray(row?.profit_rows), "net_profit");
  const payoutHistory = sortByPeriodNumericDesc(safeJsonArray(row?.profit_rows));
  const cashHistory = buildYearlyHistory(safeJsonArray(row?.cash_rows), "net_cash_flow");

  const latestPayoutRatio = payoutHistory.length ? payoutHistory[0].payout_ratio : null;
  const yieldPass = dividendYield !== null && dividendYield >= 2.5;
  const payoutRatioPass = latestPayoutRatio !== null && latestPayoutRatio >= 20 && latestPayoutRatio <= 60;
  const debtToEquity =
    borrowings !== null && (equityCapital !== null || reserves !== null)
      ? borrowings / Math.max((equityCapital || 0) + (reserves || 0), 1)
      : null;
  const debtPass = debtToEquity !== null && debtToEquity < 1;

  const latestFiveProfits = sortByPeriodNumericDesc(dividendHistory).slice(0, 5).sort((a, b) => {
    const aParts = parsePeriodParts(a?.period_numeric);
    const bParts = parsePeriodParts(b?.period_numeric);
    if (aParts.year !== bParts.year) return (aParts.year || 0) - (bParts.year || 0);
    if (aParts.month !== bParts.month) return (aParts.month || 0) - (bParts.month || 0);
    return 0;
  });
  const profitGrowth = (() => {
    if (latestFiveProfits.length < 5) return null;
    const start = latestFiveProfits[0]?.value;
    const end = latestFiveProfits[latestFiveProfits.length - 1]?.value;
    return calculateCagr(start, end, 4);
  })();
  const profitGrowthPass = profitGrowth !== null && profitGrowth > 8;

  const roeScore = scoreROE(roe);
  const roceScore = scoreROCE(roce);
  const yieldScore = scoreYield(dividendYield);
  const debtScore = scoreDebt(borrowings, reserves, totalLiabilities);
  const payoutScore = scorePayout(latestPayoutRatio);
  const profitScore = scoreConsistency(
    Number(row?.positive_profit_periods || 0),
    Number(row?.total_profit_periods || 0),
    "profit",
  );
  const cashScore = scoreCashFlow(
    Number(row?.positive_cash_periods || 0),
    Number(row?.total_cash_periods || 0),
  );

  const hardFailReasons = [
    !yieldPass ? `Dividend yield below 2.5% (${formatPercent(dividendYield)})` : null,
    !payoutRatioPass
      ? `Dividend payout ratio outside 20%-60% (${formatPercent(latestPayoutRatio)})`
      : null,
    !debtPass ? `Debt-to-equity is not below 1 (${debtToEquity === null ? 'n/a' : debtToEquity.toFixed(2)})` : null,
    !profitGrowthPass ? `5-year profit CAGR below 8% (${profitGrowth === null ? 'n/a' : profitGrowth.toFixed(2)}%)` : null,
  ].filter(Boolean);

  const score =
    yieldScore.score +
    roeScore.score +
    roceScore.score +
    payoutScore.score +
    debtScore.score +
    profitScore.score +
    cashScore.score +
    (yieldPass ? 10 : 0) +
    (payoutRatioPass ? 8 : 0) +
    (debtPass ? 10 : 0) +
    (profitGrowthPass ? 12 : 0);

  const reasons = [
    yieldScore.reason,
    roeScore.reason,
    roceScore.reason,
    payoutScore.reason,
    debtScore.reason,
    profitScore.reason,
    cashScore.reason,
    ...hardFailReasons,
  ];

  return {
    score,
    grade: pickGrade(score),
    reasons,
    core_filters: {
      dividend_yield_min_2_5: yieldPass,
      payout_ratio_20_60: payoutRatioPass,
      debt_to_equity_below_1: debtPass,
      profit_cagr_5y_above_8: profitGrowthPass,
    },
    passes_core_filters: hardFailReasons.length === 0,
    metrics: {
      dividend_yield: dividendYield,
      roe,
      roce,
      payout_ratio: latestPayoutRatio,
      profit_cagr_5y: profitGrowth,
      debt_to_equity: debtToEquity,
      positive_profit_periods: Number(row?.positive_profit_periods || 0),
      total_profit_periods: Number(row?.total_profit_periods || 0),
      positive_cash_periods: Number(row?.positive_cash_periods || 0),
      total_cash_periods: Number(row?.total_cash_periods || 0),
      borrowings,
      reserves,
      equity_capital: equityCapital,
      total_liabilities: totalLiabilities,
    },
    flags: {
      dividend_yield_positive: yieldPass,
      profit_consistent: Number(row?.positive_profit_periods || 0) >= Math.max(3, Math.ceil(Number(row?.total_profit_periods || 0) * 0.75)),
      cash_flow_consistent: Number(row?.positive_cash_periods || 0) >= Math.max(3, Math.ceil(Number(row?.total_cash_periods || 0) * 0.75)),
      debt_comfortable: debtPass,
      payout_ratio_ok: payoutRatioPass,
      profit_growth_ok: profitGrowthPass,
    },
    recommendation:
      yieldPass && payoutRatioPass && debtPass && profitGrowthPass
        ? 'Strong Dividend Candidate'
        : yieldPass && payoutRatioPass
          ? 'Dividend Watchlist'
          : dividendYield !== null && dividendYield >= 4
            ? 'High Yield Risk'
            : 'Dividend Weak',
  };
};

module.exports = {
  getDividendAnalysisRows,
  getDividendAnalysisBySymbol,
  scoreDividendCandidate,
};
