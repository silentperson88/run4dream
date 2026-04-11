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

const sortByPeriodNumericAsc = (rows = []) =>
  [...rows].sort((a, b) => {
    const aParts = parsePeriodParts(a?.period_numeric);
    const bParts = parsePeriodParts(b?.period_numeric);
    if (aParts.year !== bParts.year) return (aParts.year || 0) - (bParts.year || 0);
    if (aParts.month !== bParts.month) return (aParts.month || 0) - (bParts.month || 0);
    return 0;
  });

const sortByPeriodNumericDesc = (rows = []) => [...sortByPeriodNumericAsc(rows)].reverse();

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

const pickGrade = (score) => {
  if (score >= 80) return "A";
  if (score >= 65) return "B";
  if (score >= 50) return "C";
  return "D";
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

const buildHistory = (rows = [], key) =>
  sortByPeriodNumericAsc(rows)
    .map((row) => ({
      period_label: row?.period_label || row?.period || null,
      period_numeric: row?.period_numeric || null,
      value: toNumber(row?.[key]),
    }))
    .filter((row) => row.period_numeric);

const scoreBand = (value, bands) => {
  if (value === null) return { score: 0, reason: "No data" };
  for (const band of bands) {
    if (value >= band.min) return { score: band.score, reason: band.reason };
  }
  return { score: 0, reason: "Weak" };
};

const buildGrowthAnalysisRows = async (db = pool) => {
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

  const [ratioExists, profitExists, cashExists, balanceExists] = await Promise.all([
    tableExists("stock_fundamental_ratios_periods", db),
    tableExists("stock_fundamental_profit_loss_periods", db),
    tableExists("stock_fundamental_cash_flow_periods", db),
    tableExists("stock_fundamental_balance_sheet_periods", db),
  ]);

  const [ratiosRows, profitRows, cashRows, balanceRows] = await Promise.all([
    ratioExists
      ? fetchLatestRowsByMasterIds(
          "stock_fundamental_ratios_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, debtor_days, inventory_days, days_payable, cash_conversion_cycle, working_capital_days, roce_percent, roe_percent, last_updated_at",
          db,
        )
      : [],
    profitExists
      ? fetchAllRowsByMasterIds(
          "stock_fundamental_profit_loss_periods",
          masterIds,
          "master_id, period AS period_label, period_numeric, sales, net_profit, operating_profit, eps, dividend_payout_percent, last_updated_at, id, updated_at",
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
          "master_id, period AS period_label, period_numeric, borrowing, borrowings, reserves, equity_capital, total_liabilities, total_assets, last_updated_at",
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
    const profitHistory = sortByPeriodNumericAsc(profitByMaster[masterId] || []);
    const cashHistory = sortByPeriodNumericAsc(cashByMaster[masterId] || []);
    const latestProfit = profitHistory[profitHistory.length - 1] || null;
    const latestCash = cashHistory[cashHistory.length - 1] || null;
    const latestRatio = (ratiosByMaster[masterId] || [])[0] || null;
    const latestBalance = (balanceByMaster[masterId] || [])[0] || null;

    const salesHistory = buildHistory(profitHistory, "sales");
    const profitValueHistory = buildHistory(profitHistory, "net_profit");

    const salesCagr5y = (() => {
      if (salesHistory.length < 5) return null;
      return calculateCagr(salesHistory[0]?.value, salesHistory[salesHistory.length - 1]?.value, 4);
    })();

    const profitCagr5y = (() => {
      if (profitValueHistory.length < 5) return null;
      return calculateCagr(profitValueHistory[0]?.value, profitValueHistory[profitValueHistory.length - 1]?.value, 4);
    })();

    const roe = toNumber(latestRatio?.roe_percent ?? row?.roe);
    const roce = toNumber(latestRatio?.roce_percent ?? row?.roce);
    const borrowings = toNumber(latestBalance?.borrowings ?? latestBalance?.borrowing);
    const reserves = toNumber(latestBalance?.reserves);
    const equityCapital = toNumber(latestBalance?.equity_capital);
    const totalLiabilities = toNumber(latestBalance?.total_liabilities);
    const debtToEquity =
      borrowings !== null && (equityCapital !== null || reserves !== null)
        ? borrowings / Math.max((equityCapital || 0) + (reserves || 0), 1)
        : null;

    const positiveProfitPeriods = profitHistory.filter((item) => toNumber(item?.net_profit) > 0).length;
    const totalProfitPeriods = profitHistory.length;
    const positiveCashPeriods = cashHistory.filter((item) => toNumber(item?.net_cash_flow) > 0).length;
    const totalCashPeriods = cashHistory.length;

    return {
      ...row,
      latest_profit_period: latestProfit?.period_label || null,
      latest_profit_period_numeric: latestProfit?.period_numeric || null,
      latest_net_profit: latestProfit?.net_profit ?? null,
      latest_sales: latestProfit?.sales ?? null,
      latest_eps: latestProfit?.eps ?? null,
      latest_cash_period: latestCash?.period_label || null,
      latest_cash_period_numeric: latestCash?.period_numeric || null,
      cash_from_operating_activity: latestCash?.cash_from_operating_activity ?? null,
      net_cash_flow: latestCash?.net_cash_flow ?? null,
      latest_balance_period: latestBalance?.period_label || null,
      latest_balance_period_numeric: latestBalance?.period_numeric || null,
      latest_borrowings: borrowings,
      latest_reserves: reserves,
      latest_equity_capital: equityCapital,
      latest_total_liabilities: totalLiabilities,
      roe_percent: roe,
      roce_percent: roce,
      sales_history: salesHistory,
      profit_history: profitValueHistory,
      cash_history: cashHistory,
      positive_profit_periods: positiveProfitPeriods,
      total_profit_periods: totalProfitPeriods,
      positive_cash_periods: positiveCashPeriods,
      total_cash_periods: totalCashPeriods,
      growth_metrics: {
        sales_cagr_5y: salesCagr5y,
        profit_cagr_5y: profitCagr5y,
        roe,
        roce,
        debt_to_equity: debtToEquity,
      },
    };
  });
};

const scoreGrowthCandidate = (row) => {
  const salesCagr5y = toNumber(row?.growth_metrics?.sales_cagr_5y);
  const profitCagr5y = toNumber(row?.growth_metrics?.profit_cagr_5y);
  const roe = toNumber(row?.growth_metrics?.roe ?? row?.roe_percent ?? row?.roe);
  const roce = toNumber(row?.growth_metrics?.roce ?? row?.roce_percent ?? row?.roce);
  const debtToEquity = toNumber(row?.growth_metrics?.debt_to_equity);

  const salesScore = scoreBand(salesCagr5y, [
    { min: 20, score: 20, reason: "Excellent sales growth" },
    { min: 12, score: 16, reason: "Strong sales growth" },
    { min: 8, score: 12, reason: "Healthy sales growth" },
    { min: 0, score: 6, reason: "Low sales growth" },
  ]);
  const profitScore = scoreBand(profitCagr5y, [
    { min: 25, score: 25, reason: "Excellent profit growth" },
    { min: 15, score: 20, reason: "Strong profit growth" },
    { min: 8, score: 15, reason: "Healthy profit growth" },
    { min: 0, score: 8, reason: "Low profit growth" },
  ]);
  const roeScore = scoreBand(roe, [
    { min: 20, score: 15, reason: "Excellent ROE" },
    { min: 15, score: 12, reason: "Strong ROE" },
    { min: 10, score: 8, reason: "Decent ROE" },
    { min: 0, score: 4, reason: "Weak ROE" },
  ]);
  const roceScore = scoreBand(roce, [
    { min: 20, score: 15, reason: "Excellent ROCE" },
    { min: 15, score: 12, reason: "Strong ROCE" },
    { min: 10, score: 8, reason: "Decent ROCE" },
    { min: 0, score: 4, reason: "Weak ROCE" },
  ]);

  const debtScore = (() => {
    if (debtToEquity === null) return { score: 0, reason: "No debt data" };
    if (debtToEquity < 0.5) return { score: 12, reason: "Very low debt" };
    if (debtToEquity < 1) return { score: 10, reason: "Healthy debt" };
    if (debtToEquity < 2) return { score: 5, reason: "Moderate debt" };
    return { score: 0, reason: "High debt" };
  })();

  const profitConsistency = Number(row?.positive_profit_periods || 0);
  const totalProfitPeriods = Number(row?.total_profit_periods || 0);
  const cashConsistency = Number(row?.positive_cash_periods || 0);
  const totalCashPeriods = Number(row?.total_cash_periods || 0);

  const profitConsistent = totalProfitPeriods > 0 ? profitConsistency / totalProfitPeriods >= 0.75 : false;
  const cashConsistent = totalCashPeriods > 0 ? cashConsistency / totalCashPeriods >= 0.75 : false;

  const consistencyScore = (profitConsistent ? 8 : 0) + (cashConsistent ? 8 : 0);

  const score =
    salesScore.score +
    profitScore.score +
    roeScore.score +
    roceScore.score +
    debtScore.score +
    consistencyScore;

  const reasons = [
    salesScore.reason,
    profitScore.reason,
    roeScore.reason,
    roceScore.reason,
    debtScore.reason,
    profitConsistent ? "Profit history is reasonably consistent" : "Profit history is uneven",
    cashConsistent ? "Cash flow history is reasonably consistent" : "Cash flow history is uneven",
  ];

  const recommendation =
    salesCagr5y !== null && salesCagr5y >= 12 && profitCagr5y !== null && profitCagr5y >= 15 && debtToEquity !== null && debtToEquity < 1
      ? "Strong Growth Candidate"
      : salesCagr5y !== null && salesCagr5y >= 8 && profitCagr5y !== null && profitCagr5y >= 8
        ? "Growth Watchlist"
        : "Growth Weak";

  return {
    score,
    grade: pickGrade(score),
    recommendation,
    reasons,
    metrics: {
      sales_cagr_5y: salesCagr5y,
      profit_cagr_5y: profitCagr5y,
      roe,
      roce,
      debt_to_equity: debtToEquity,
      positive_profit_periods: profitConsistency,
      total_profit_periods: totalProfitPeriods,
      positive_cash_periods: cashConsistency,
      total_cash_periods: totalCashPeriods,
    },
    flags: {
      sales_growth_strong: salesCagr5y !== null && salesCagr5y >= 12,
      profit_growth_strong: profitCagr5y !== null && profitCagr5y >= 15,
      roe_strong: roe !== null && roe >= 15,
      roce_strong: roce !== null && roce >= 15,
      debt_comfortable: debtToEquity !== null && debtToEquity < 1,
    },
  };
};

const getGrowthAnalysisRows = async ({ limit = 50 } = {}, db = pool) => {
  const rows = await buildGrowthAnalysisRows(db);
  return rows.slice(0, Number(limit) || 50);
};

const getGrowthAnalysisBySymbol = async (symbol, db = pool) => {
  const rows = await buildGrowthAnalysisRows(db);
  return rows.find((row) => String(row.symbol || "").toUpperCase() === String(symbol || "").trim().toUpperCase()) || null;
};

module.exports = {
  getGrowthAnalysisRows,
  getGrowthAnalysisBySymbol,
  scoreGrowthCandidate,
};
