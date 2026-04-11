const { pool } = require("../config/db");
const activeStockRepo = require("../repositories/activeStocks.repository");
const { buildValueAnalysisRows } = require("./valueAnalysis.service");

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const avg = (arr = []) => {
  const vals = arr.map(toNumber).filter((v) => v !== null);
  if (!vals.length) return null;
  return vals.reduce((s, v) => s + v, 0) / vals.length;
};

const cagr = (start, end, years) => {
  const a = toNumber(start);
  const b = toNumber(end);
  if (a === null || b === null || a <= 0 || b <= 0 || !years) return null;
  return (Math.pow(b / a, 1 / years) - 1) * 100;
};

const pct = (from, to) => {
  const a = toNumber(from);
  const b = toNumber(to);
  if (a === null || b === null || a === 0) return null;
  return ((b - a) / Math.abs(a)) * 100;
};

const formatCurrency = (value, digits = 2) => {
  const n = toNumber(value);
  if (n === null) return "n/a";
  return `Rs. ${n.toFixed(digits)}`;
};

const sortPeriod = (rows = []) =>
  [...rows].sort((a, b) => {
    const ap = String(a?.period_numeric || "").match(/^(\d{2})-(\d{4})$/);
    const bp = String(b?.period_numeric || "").match(/^(\d{2})-(\d{4})$/);
    const ay = ap ? Number(ap[2]) : 0;
    const by = bp ? Number(bp[2]) : 0;
    if (ay !== by) return ay - by;
    const am = ap ? Number(ap[1]) : 0;
    const bm = bp ? Number(bp[1]) : 0;
    return am - bm;
  });

const groupByMaster = (rows = []) =>
  rows.reduce((acc, row) => {
    const key = String(row?.master_id || "");
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});

const exists = async (table, db = pool) => {
  const { rows } = await db.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1 LIMIT 1`,
    [table],
  );
  return rows.length > 0;
};

const getExistingColumns = async (table, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
      ORDER BY ordinal_position
    `,
    [table],
  );
  return rows.map((row) => String(row.column_name || "").trim()).filter(Boolean);
};

const fetchByMasterIds = async (table, masterIds, desiredColumns, db = pool) => {
  if (!masterIds.length) return [];
  const existingColumns = await getExistingColumns(table, db);
  const existingSet = new Set(existingColumns);
  const selectedColumns = desiredColumns.filter((column) => existingSet.has(column));
  const selectList = ["master_id", ...selectedColumns].join(", ");
  const { rows } = await db.query(
    `
      SELECT ${selectList}
      FROM ${table}
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id
    `,
    [masterIds.map(Number)],
  );
  return rows;
};

const tierLabel = (tier) => ({ small: "Pivot Small", mid: "Pivot Mid", large: "Pivot Large" }[tier] || "Pivot");
const gradeOf = (score) => (score >= 80 ? "PIVOT Ready" : score >= 65 ? "PIVOT Watch" : score >= 50 ? "PIVOT Weak" : "Not a PIVOT");
const normGrade = (g) => String(g || "").trim().toUpperCase();
const matchGrade = (row, filter = "ALL") => !filter || filter === "ALL" || normGrade(row?.analysis?.grade) === normGrade(filter);
const matchScore = (row, minScore = null) => {
  const n = toNumber(minScore);
  return n === null || Number(row?.analysis?.score || 0) >= n;
};

const pickTier = (price) => {
  const p = toNumber(price);
  if (p === null) return null;
  if (p >= 2 && p < 30) return "small";
  if (p >= 30 && p < 500) return "mid";
  if (p >= 500 && p <= 2000) return "large";
  return null;
};

const buildQuarterStats = (rows = []) => {
  const q = sortPeriod(rows).map((row) => ({
    ...row,
    revenue: toNumber(row.revenue ?? row.sales),
    opm: toNumber(row.opm_percent),
    profit: toNumber(row.net_profit),
    eps: toNumber(row.eps),
    interest: toNumber(row.interest),
  }));
  return {
    rows: q,
    last1: q.at(-1) || null,
    last2: q.slice(-2),
    prev2: q.slice(-4, -2),
    last3: q.slice(-3),
    last4: q.slice(-4),
    prev4: q.slice(-8, -4),
    prev12: q.slice(-14, -2),
  };
};

const buildBalanceStats = (rows = []) => {
  const b = sortPeriod(rows).map((row) => ({
    ...row,
    borrowings: toNumber(row.borrowings ?? row.borrowing),
    reserves: toNumber(row.reserves),
    equity: toNumber(row.equity_capital),
  }));
  const pickEq = (r) => (toNumber(r?.reserves) || 0) + (toNumber(r?.equity) || 0);
  const de = (r) => {
    const e = pickEq(r);
    const d = toNumber(r?.borrowings);
    return d !== null && e > 0 ? d / e : null;
  };
  return {
    rows: b,
    latest: b.at(-1) || null,
    prev: b.length > 1 ? b.at(-2) : null,
    twoYearsAgo: b.length > 2 ? b.at(-3) : null,
    currentBorrowings: toNumber(b.at(-1)?.borrowings),
    prevBorrowings: toNumber(b.at(-2)?.borrowings),
    twoYearsBorrowings: toNumber(b.at(-3)?.borrowings),
    currentNetWorth: pickEq(b.at(-1)),
    prevNetWorth: pickEq(b.at(-2)),
    currentDE: de(b.at(-1)),
    prevDE: de(b.at(-2)),
    twoYearsDE: de(b.at(-3)),
  };
};

const buildPivotAnalysisRow = (baseRow, activeRow, quarterStats, balanceStats) => {
  const price = toNumber(activeRow?.ltp ?? baseRow.current_price);
  const week52Low = toNumber(activeRow?.week52Low ?? baseRow.week52_low);
  const week52High = toNumber(activeRow?.week52High ?? baseRow.week52_high);
  const tier = pickTier(price);
  if (!tier) return null;

  const vm = baseRow.value_metrics || {};
  const cashHistory = baseRow.cash_history || [];
  const ratioHistory = baseRow.ratio_history || [];
  const priceLowGap = week52Low ? ((price - week52Low) / week52Low) * 100 : null;
  const nearLow = priceLowGap !== null
    ? (tier === "small" ? priceLowGap <= 40 : tier === "mid" ? priceLowGap <= 35 : priceLowGap <= 25)
    : false;

  const q = quarterStats.rows;
  const qLast2 = quarterStats.last2;
  const qPrev2 = quarterStats.prev2;
  const qLast3 = quarterStats.last3;
  const qLast4 = quarterStats.last4;
  const qPrev4 = quarterStats.prev4;
  const qPrev12 = quarterStats.prev12;

  const recent2QRevenueAvg = avg(qLast2.map((r) => r.revenue));
  const avgPrev3YQuarterRevenue = avg(qPrev12.map((r) => r.revenue));
  const lastYearOpmAvg = avg(qLast4.map((r) => r.opm));
  const prevYearOpmAvg = avg(qPrev4.map((r) => r.opm));
  const yearBeforeOpmAvg = avg(q.slice(-12, -8).map((r) => r.opm));
  const latestQ = q.at(-1) || {};
  const prev3QAvgRevenue = avg(q.slice(-4, -1).map((r) => r.revenue));
  const latest2QProfitPositive = qLast2.length === 2 && qLast2.every((r) => r.profit !== null && r.profit > 0);
  const prev2QProfitNegative = qPrev2.length === 2 && qPrev2.every((r) => r.profit !== null && r.profit < 0);
  const revenue3Consecutive = qLast3.length === 3 && qLast3.every((r, idx) => idx === 0 || r.revenue > qLast3[idx - 1].revenue);
  const currentDE = balanceStats.currentDE;
  const prevDE = balanceStats.prevDE;
  const twoYearsDE = balanceStats.twoYearsDE;
  const netWorth = balanceStats.currentNetWorth;
  const currentBorrowings = balanceStats.currentBorrowings;
  const twoYearsBorrowings = balanceStats.twoYearsBorrowings;
  const promoterHolding = vm.promoters;
  const promoterTrend = vm.promoter_holding_trend;
  const promoterDrop = vm.promoter_max_quarter_drop_4q;
  const fiiTrend = vm.fii_trend;
  const diiTrend = vm.dii_trend;
  const publicTrend = vm.public_holding_trend;
  const ocfYears = cashHistory.map((x) => toNumber(x.cash_from_operating_activity)).filter((x) => x !== null);
  const latestOcf = vm.cash_from_operating_activity ?? null;
  const latestNetProfit = latestQ.profit ?? baseRow.latest_net_profit ?? null;

  const signals = [];
  const flags = [];
  const rejects = [];
  const add = (bucket, item) => bucket.push(item);

  if (tier === "small") {
    const l1 = [
      {
        code: "S1",
        label: "2Q revenue accelerating",
        value: recent2QRevenueAvg,
        threshold: "> 20% vs 3Y avg quarterly revenue",
        passed:
          recent2QRevenueAvg !== null &&
          avgPrev3YQuarterRevenue !== null &&
          recent2QRevenueAvg > avgPrev3YQuarterRevenue * 1.2,
      },
      {
        code: "S2",
        label: "OPM expanding this year",
        value: lastYearOpmAvg,
        threshold: "> last year by 3%",
        passed: lastYearOpmAvg !== null && prevYearOpmAvg !== null && lastYearOpmAvg > prevYearOpmAvg + 3,
      },
      {
        code: "S3",
        label: "Debt reduced vs 2 years ago",
        value: currentBorrowings,
        threshold: "< 75% of 2Y borrowings",
        passed: currentBorrowings !== null && twoYearsBorrowings !== null && currentBorrowings <= twoYearsBorrowings * 0.75,
      },
      {
        code: "S4",
        label: "Last 2Q profit turnaround",
        value: latestQ.profit,
        threshold: "last 2Q positive after negative base",
        passed: latest2QProfitPositive && prev2QProfitNegative,
      },
      {
        code: "S5",
        label: "Revenue growing 3 quarters",
        value: latestQ.revenue,
        threshold: "3 consecutive increasing quarters",
        passed: revenue3Consecutive,
      },
    ];
    l1.forEach((item) => add(signals, { ...item, points: item.passed ? 8 : 0 }));

    const d1 = currentDE !== null && currentDE >= 2.0;
    const d2 = promoterHolding !== null && promoterHolding <= 25;
    const d3 = promoterDrop !== null && promoterDrop >= 8;
    const d4 = !(ocfYears.slice(-2).some((x) => x !== null && x > 0));
    const d5 = netWorth !== null && netWorth <= 0;
  add(flags, { code: "L2-1", label: "Debt to equity", value: currentDE, threshold: "< 2.0", passed: !d1, type: d1 ? "flag" : "pass" });
  add(flags, { code: "L2-2", label: "Promoter holding", value: promoterHolding, threshold: "> 25%", passed: !d2, type: d2 ? "reject" : "pass" });
    add(flags, { code: "L2-3", label: "Promoter drop", value: promoterDrop, threshold: "< 8%", passed: !d3, type: d3 ? "flag" : "pass" });
    add(flags, { code: "L2-4", label: "Operating cash flow", value: ocfYears.slice(-2), threshold: "> 0 in at least 1 of last 2 years", passed: !d4, type: d4 ? "flag" : "pass" });
    add(flags, { code: "L2-5", label: "Net worth", value: netWorth, threshold: "> 0", passed: !d5, type: d5 ? "reject" : "pass" });
    if (d2) rejects.push("Promoter holding too low");
    if (d5) rejects.push("Negative net worth");

    const p1 = priceLowGap !== null && priceLowGap <= 10;
    const p2 = nearLow;
    const p3 = priceLowGap !== null && priceLowGap <= 20 && (vm.profit_cagr_3y === null || vm.profit_cagr_3y > 0);
    add(signals, { code: "P1", label: "Price near low", value: priceLowGap, threshold: "< 10% from 52W low", passed: p1, points: p1 ? 10 : 0 });
    add(signals, { code: "P2", label: "Near 52W low", value: priceLowGap, threshold: "within 40% of 52W low", passed: p2, points: p2 ? 10 : 0 });
    add(signals, { code: "P3", label: "Flat / slow reaction", value: priceLowGap, threshold: "flat with profit growth", passed: p3, points: p3 ? 10 : 0 });
  }

  if (tier === "mid") {
    const l1 = [
      {
        code: "S1",
        label: "Recent revenue acceleration",
        value: recent2QRevenueAvg,
        threshold: "> 3Y CAGR by 15%",
        passed:
          recent2QRevenueAvg !== null &&
          avgPrev3YQuarterRevenue !== null &&
          recent2QRevenueAvg > avgPrev3YQuarterRevenue * 1.15,
      },
      {
        code: "S2",
        label: "OPM expanding 2 years",
        value: lastYearOpmAvg,
        threshold: "2 consecutive yearly expansion",
        passed: lastYearOpmAvg !== null && prevYearOpmAvg !== null && yearBeforeOpmAvg !== null && lastYearOpmAvg > prevYearOpmAvg && prevYearOpmAvg > yearBeforeOpmAvg,
      },
      {
        code: "S3",
        label: "Debt / equity improving",
        value: currentDE,
        threshold: "down by > 0.3 in 1 year",
        passed: currentDE !== null && prevDE !== null && currentDE <= prevDE - 0.3,
      },
      {
        code: "S4",
        label: "Profit CAGR with flat stock",
        value: vm.profit_cagr_3y,
        threshold: "> 20% while price flat",
        passed: vm.profit_cagr_3y !== null && vm.profit_cagr_3y > 20 && (priceLowGap === null || priceLowGap <= 15),
      },
      {
        code: "S5",
        label: "ROCE improving",
        value: vm.roce,
        threshold: "this year > last year > year before",
        passed: (() => {
          const roceSeries = ratioHistory.map((r) => toNumber(r.roce_percent)).filter((v) => v !== null);
          return roceSeries.length >= 3 && roceSeries.at(-1) > roceSeries.at(-2) && roceSeries.at(-2) > roceSeries.at(-3);
        })(),
      },
    ];
    l1.forEach((item) => add(signals, { ...item, points: item.passed ? 8 : 0 }));

    const r1 = currentDE !== null && currentDE >= 1.5;
    const r2 = promoterHolding !== null && promoterHolding <= 35;
    const r3 = promoterDrop !== null && promoterDrop >= 5;
    const f1 = !(ocfYears.slice(-3).filter((x) => x !== null && x > 0).length >= 2);
    const f2 = debtorDaysMissingOrFlag(vm.debtor_days, 150);
    const f3 = interestMissingOrFlag(vm.interest_coverage, 2);
    add(flags, { code: "L2-1", label: "Debt to equity", value: currentDE, threshold: "< 1.5", passed: !r1, type: r1 ? "reject" : "pass" });
    add(flags, { code: "L2-2", label: "Promoter holding", value: promoterHolding, threshold: "> 35%", passed: !r2, type: r2 ? "reject" : "pass" });
    add(flags, { code: "L2-3", label: "Promoter drop", value: promoterDrop, threshold: "< 5%", passed: !r3, type: r3 ? "reject" : "pass" });
    add(flags, { code: "L2-4", label: "Operating cash flow", value: ocfYears.slice(-3), threshold: "> 0 in 2 of last 3 years", passed: !f1, type: f1 ? "flag" : "pass" });
    add(flags, { code: "L2-5", label: "Debtor days", value: vm.debtor_days, threshold: "< 150 days", passed: !f2, type: f2 ? "flag" : "pass" });
    add(flags, { code: "L2-6", label: "Interest coverage", value: vm.interest_coverage, threshold: "> 2x", passed: !f3, type: f3 ? "flag" : "pass" });
    if (r1) rejects.push("Debt too high");
    if (r2) rejects.push("Promoter holding too low");
    if (r3) rejects.push("Promoter drop too high");

    const p1 = priceLowGap !== null && vm.profit_cagr_3y !== null && priceLowGap <= 15 && vm.profit_cagr_3y > 20;
    const p2 = vm.pe_vs_industry !== null ? vm.pe_vs_industry < 1 : nearLow;
    const p3 = nearLow;
    const p4 = priceLowGap !== null && priceLowGap <= 20;
    add(signals, { code: "P1", label: "Price lagging growth", value: priceLowGap, threshold: "< 15% from low while profit > 20%", passed: p1, points: p1 ? 10 : 0 });
    add(signals, { code: "P2", label: "Below industry P/E", value: vm.pe_vs_industry, threshold: "< industry average", passed: p2, points: p2 ? 10 : 0 });
    add(signals, { code: "P3", label: "Near 52W low", value: priceLowGap, threshold: "within 35% of low", passed: p3, points: p3 ? 10 : 0 });
    add(signals, { code: "P4", label: "Sideways range", value: priceLowGap, threshold: "price not reacted yet", passed: p4, points: p4 ? 10 : 0 });
  }

  if (tier === "large") {
    const l1 = [
      {
        code: "S1",
        label: "Revenue reaccelerating",
        value: latestQ.revenue,
        threshold: "> previous 3Q avg by 20%",
        passed: latestQ.revenue !== null && prev3QAvgRevenue !== null && latestQ.revenue > prev3QAvgRevenue * 1.2,
      },
      {
        code: "S2",
        label: "OPM expanding after compression",
        value: lastYearOpmAvg,
        threshold: "2+ years of compression ending",
        passed: lastYearOpmAvg !== null && prevYearOpmAvg !== null && yearBeforeOpmAvg !== null && lastYearOpmAvg > prevYearOpmAvg && prevYearOpmAvg > yearBeforeOpmAvg,
      },
      {
        code: "S3",
        label: "Debt nearly eliminated",
        value: currentDE,
        threshold: "< 0.3 from > 0.8",
        passed: currentDE !== null && twoYearsDE !== null && currentDE < 0.3 && twoYearsDE > 0.8,
      },
      {
        code: "S4",
        label: "New segment growth",
        value: null,
        threshold: "> 30%",
        passed: false,
        missing: true,
      },
      {
        code: "S5",
        label: "Profit CAGR strong, P/E reasonable",
        value: vm.profit_cagr_3y,
        threshold: "> 25% and below industry P/E",
        passed: vm.profit_cagr_3y !== null && vm.profit_cagr_3y > 25 && (vm.pe_vs_industry === null || vm.pe_vs_industry < 1),
      },
    ];
    l1.forEach((item) => add(signals, { ...item, points: item.passed ? 8 : 0 }));

    const r1 = currentDE !== null && currentDE >= 1.0;
    const r2 = promoterHolding !== null && promoterHolding <= 40;
    const r3 = promoterDrop !== null && promoterDrop >= 3;
    const r4 = !(ocfYears.length >= 3 && ocfYears.slice(-3).every((x) => x !== null && x > 0));
    const r5 = debtorDaysMissingOrFlag(vm.debtor_days, 120);
    const f1 = interestMissingOrFlag(vm.interest_coverage, 3);
    const f2 = vm.roce !== null && vm.roce <= 12;
    add(flags, { code: "L2-1", label: "Debt to equity", value: currentDE, threshold: "< 1.0", passed: !r1, type: r1 ? "reject" : "pass" });
    add(flags, { code: "L2-2", label: "Promoter holding", value: promoterHolding, threshold: "> 40%", passed: !r2, type: r2 ? "reject" : "pass" });
    add(flags, { code: "L2-3", label: "Promoter drop", value: promoterDrop, threshold: "< 3%", passed: !r3, type: r3 ? "reject" : "pass" });
    add(flags, { code: "L2-4", label: "Operating cash flow", value: ocfYears.slice(-3), threshold: "> 0 all last 3 years", passed: !r4, type: r4 ? "reject" : "pass" });
    add(flags, { code: "L2-5", label: "Debtor days", value: vm.debtor_days, threshold: "< 120 days", passed: !r5, type: r5 ? "reject" : "pass" });
    add(flags, { code: "L2-6", label: "Interest coverage", value: vm.interest_coverage, threshold: "> 3x", passed: !f1, type: f1 ? "flag" : "pass" });
    add(flags, { code: "L2-7", label: "ROCE", value: vm.roce, threshold: "> 12%", passed: !f2, type: f2 ? "flag" : "pass" });
    if (r1) rejects.push("Debt too high");
    if (r2) rejects.push("Promoter holding too low");
    if (r3) rejects.push("Promoter drop too high");
    if (r4) rejects.push("OCF not positive for 3 years");
    if (r5) rejects.push("Debtor days too high");

    const p1 = priceLowGap !== null && vm.profit_cagr_3y !== null && priceLowGap <= 10 && vm.profit_cagr_3y > 25;
    const p2 = vm.pe_vs_industry !== null && vm.pe_vs_industry < 1;
    const p3 = nearLow;
    add(signals, { code: "P1", label: "Price lagging strong growth", value: priceLowGap, threshold: "< 10% from low while profit > 25%", passed: p1, points: p1 ? 10 : 0 });
    add(signals, { code: "P2", label: "Below industry P/E", value: vm.pe_vs_industry, threshold: "< industry average", passed: p2, points: p2 ? 10 : 0 });
    add(signals, { code: "P3", label: "Near 52W low", value: priceLowGap, threshold: "within 25% of low", passed: p3, points: p3 ? 10 : 0 });
  }

  const layer1Score = signals.filter((x) => String(x.code || "").startsWith("S")).reduce((sum, x) => sum + Number(x.points || 0), 0);
  const layer3Hits = signals.filter((x) => String(x.code || "").startsWith("P") && x.passed).length;
  const layer3Score = layer3Hits >= 2 ? 20 : layer3Hits === 1 ? 10 : 0;
  const layer2Penalty = flags.filter((x) => x.type === "flag" && !x.passed).length * 5;
  const layer2Score = rejects.length ? 0 : Math.max(0, 30 - layer2Penalty);
  const promoterBonus = promoterTrend === "increasing" ? 10 : promoterTrend === "stable" ? 5 : 0;

  const warningList = [];
  if (promoterDrop !== null && promoterDrop > 1.5) warningList.push({ code: "W1", severity: "critical", title: "Promoter dropping", description: "Promoter holding dropped sharply in a quarter." });
  if (fiiTrend === "decreasing" && diiTrend === "decreasing" && promoterTrend === "stable") warningList.push({ code: "W2", severity: "critical", title: "Institutional exit", description: "FII and DII are both exiting while promoter is stable." });
  if (q.length >= 4) {
    const prev3 = q.slice(-4, -1).map((r) => r.revenue);
    const prev3Avg = avg(prev3);
    const latestRevenue = q.at(-1)?.revenue ?? null;
    if (latestRevenue !== null && prev3Avg !== null && latestRevenue > prev3Avg * 1.2 && prev3.every((v) => v !== null && Math.abs(v - prev3Avg) <= prev3Avg * 0.08)) {
      warningList.push({ code: "W3", severity: "critical", title: "Revenue one-off", description: "Latest quarter is strong while prior 3 quarters were flat." });
    }
  }
  if (latestOcf !== null && latestNetProfit !== null && latestNetProfit > 0 && latestOcf < latestNetProfit * 0.5) warningList.push({ code: "W4", severity: "moderate", title: "CFO mismatch", description: "Cash flow is converting poorly from profit." });
  if ((vm.fii_net_change_4q || 0) < 0 && (vm.dii_net_change_4q || 0) < 0) warningList.push({ code: "W5", severity: "critical", title: "Institutional exit", description: "FII and DII both show negative net change over the last 4 quarters." });
  if (vm.debtor_days !== null && ratioHistory.length >= 2) {
    const ds = ratioHistory.map((x) => toNumber(x.debtor_days)).filter((x) => x !== null);
    if (ds.length >= 2 && ds.at(-1) - ds.at(-2) > 30) warningList.push({ code: "W6", severity: "minor", title: "Debtor spike", description: "Debtor days increased sharply year over year." });
  }

  const penalty = warningList.reduce((sum, w) => sum + (w.severity === "critical" ? 5 : w.severity === "moderate" ? 3 : 1), 0);
  const score = Math.max(0, Math.min(100, layer1Score + layer2Score + layer3Score + promoterBonus - penalty));
  const grade = gradeOf(score);

  const entryResistance = week52High ? week52High * 0.98 : price ? price * 1.08 : null;
  const stopLoss = week52Low ? week52Low * 0.97 : null;

  return {
    pivot_tier: tier,
    tier_label: tierLabel(tier),
    pivot_metrics: {
      current_price: price,
      week52_low: week52Low,
      week52_high: week52High,
      price_low_gap_percent: priceLowGap,
      revenue_cagr_3y: vm.revenue_cagr_3y,
      profit_cagr_3y: vm.profit_cagr_3y,
      opm_percent: vm.opm_percent,
      roe: vm.roe,
      roce: vm.roce,
      debt_to_equity: currentDE,
      promoter_holding: promoterHolding,
      promoter_holding_trend: promoterTrend,
      promoter_max_quarter_drop_4q: promoterDrop,
      fii_trend: fiiTrend,
      dii_trend: diiTrend,
      public_holding_trend: publicTrend,
      interest_coverage: vm.interest_coverage,
      debtor_days: vm.debtor_days,
      pe_ratio: vm.pe_ratio,
      pe_vs_industry: vm.pe_vs_industry,
      price_to_book: vm.price_to_book,
      ev_ebitda: vm.ev_ebitda,
      latest_q_revenue: latestQ.revenue,
      latest_q_profit: latestQ.profit,
      latest_q_opm: latestQ.opm,
      last_year_opm_avg: lastYearOpmAvg,
      prev_year_opm_avg: prevYearOpmAvg,
      revenue_2q_avg: recent2QRevenueAvg,
      revenue_3y_quarter_avg: avgPrev3YQuarterRevenue,
      current_net_worth: balanceStats.currentNetWorth,
    },
    analysis: {
      score,
      grade,
      recommendation: grade,
      hard_reject: rejects.length > 0,
      layer_scores: { layer1: layer1Score, layer2: layer2Score, layer3: layer3Score, bonus: promoterBonus, penalty },
      layer1: signals.filter((x) => String(x.code || "").startsWith("S")),
      layer2: flags,
      layer3: signals.filter((x) => String(x.code || "").startsWith("P")),
      warnings: warningList,
      missing_data: [
        quarterStats.rows.length < 4 ? "Quarterly history below 1 year" : null,
        vm.pe_vs_industry === null ? "Industry P/E missing" : null,
        priceLowGap === null ? "Price history missing" : null,
      ].filter(Boolean),
      entry_trigger: {
        resistance: entryResistance,
        volume_trigger: null,
        stop_loss: stopLoss,
      note: entryResistance ? `Break above ${formatCurrency(entryResistance, 2)}; confirm with volume` : "Technical confirmation pending",
      },
      tier_scores: {
        small: tier === "small" ? score : 0,
        mid: tier === "mid" ? score : 0,
        large: tier === "large" ? score : 0,
      },
      metrics: {
        current_price: price,
        week52_low: week52Low,
        week52_high: week52High,
        price_low_gap_percent: priceLowGap,
        revenue_cagr_3y: vm.revenue_cagr_3y,
        profit_cagr_3y: vm.profit_cagr_3y,
        opm_percent: vm.opm_percent,
        roe: vm.roe,
        roce: vm.roce,
        debt_to_equity: currentDE,
        promoter_holding: promoterHolding,
        promoter_holding_trend: promoterTrend,
        promoter_max_quarter_drop_4q: promoterDrop,
        fii_trend: fiiTrend,
        dii_trend: diiTrend,
        public_holding_trend: publicTrend,
        interest_coverage: vm.interest_coverage,
        debtor_days: vm.debtor_days,
        pe_ratio: vm.pe_ratio,
        pe_vs_industry: vm.pe_vs_industry,
        price_to_book: vm.price_to_book,
        ev_ebitda: vm.ev_ebitda,
      },
    },
  };
};

const buildPivotAnalysisRows = async ({ includeRejected = false } = {}, db = pool) => {
  const baseRows = (await buildValueAnalysisRows({ tier1Only: false }, db)).filter(Boolean);
  if (!baseRows.length) return [];

  const masterIds = baseRows.map((r) => Number(r.master_id)).filter((n) => Number.isFinite(n));
  const [activeRows, qExists, bExists] = await Promise.all([
    activeStockRepo.listByMasterIds(masterIds, db),
    exists("stock_fundamental_quarterly_results", db),
    exists("stock_fundamental_balance_sheet_periods", db),
  ]);
  const [quarterRows, balanceRows] = await Promise.all([
    qExists
      ? fetchByMasterIds(
          "stock_fundamental_quarterly_results",
          masterIds,
          ["period", "period_numeric", "sales", "revenue", "opm_percent", "net_profit", "eps", "interest"],
          db,
        )
      : [],
    bExists
      ? fetchByMasterIds(
          "stock_fundamental_balance_sheet_periods",
          masterIds,
          ["period", "period_numeric", "borrowing", "borrowings", "reserves", "equity_capital"],
          db,
        )
      : [],
  ]);

  const activeByMaster = groupByMaster(activeRows);
  const qByMaster = groupByMaster(quarterRows);
  const bByMaster = groupByMaster(balanceRows);

  const rows = baseRows
    .map((baseRow) => {
      const active = (activeByMaster[String(baseRow.master_id)] || [])[0] || null;
      const quarterStats = buildQuarterStats(qByMaster[String(baseRow.master_id)] || []);
      const balanceStats = buildBalanceStats(bByMaster[String(baseRow.master_id)] || []);
      const row = buildPivotAnalysisRow(baseRow, active, quarterStats, balanceStats);
      if (!row) return null;
      return {
        ...baseRow,
        current_price: row.pivot_metrics.current_price,
        week52_low: row.pivot_metrics.week52_low,
        week52_high: row.pivot_metrics.week52_high,
        pivot_tier: row.pivot_tier,
        pivot_metrics: row.pivot_metrics,
        analysis: row.analysis,
      };
    })
    .filter(Boolean);

  const filtered = includeRejected ? rows : rows.filter((row) => !row.analysis.hard_reject && row.analysis.score >= 50);
  return filtered;
};

const getPivotAnalysisBuckets = async ({ limit = 50, grade = "ALL", minScore = null, includeRejected = false } = {}, db = pool) => {
  const rows = (await buildPivotAnalysisRows({ includeRejected }, db))
    .filter((row) => matchGrade(row, grade))
    .filter((row) => matchScore(row, minScore));

  const sortOverall = (a, b) => {
    const scoreDiff = Number(b.analysis.score || 0) - Number(a.analysis.score || 0);
    if (scoreDiff !== 0) return scoreDiff;
    const l1Diff = Number(b.analysis.layer_scores.layer1 || 0) - Number(a.analysis.layer_scores.layer1 || 0);
    if (l1Diff !== 0) return l1Diff;
    return String(a.symbol || "").localeCompare(String(b.symbol || ""));
  };

  const byTier = (tier) =>
    [...rows.filter((r) => r.pivot_tier === tier)].sort(sortOverall).slice(0, Number(limit) || 50);

  const overallRows = [...rows].sort(sortOverall).slice(0, Number(limit) || 50);

  return {
    total: rows.length,
    overallRows,
    tierRows: {
      small: byTier("small"),
      mid: byTier("mid"),
      large: byTier("large"),
    },
    summary: {
      small: rows.filter((r) => r.pivot_tier === "small").length,
      mid: rows.filter((r) => r.pivot_tier === "mid").length,
      large: rows.filter((r) => r.pivot_tier === "large").length,
      ready: rows.filter((r) => r.analysis.grade === "PIVOT Ready").length,
      watch: rows.filter((r) => r.analysis.grade === "PIVOT Watch").length,
      weak: rows.filter((r) => r.analysis.grade === "PIVOT Weak").length,
      reject: rows.filter((r) => r.analysis.grade === "Not a PIVOT").length,
    },
  };
};

const getPivotAnalysisBySymbol = async (symbol, db = pool) => {
  const rows = await buildPivotAnalysisRows({ includeRejected: true }, db);
  return rows.find((row) => String(row.symbol || "").toUpperCase() === String(symbol || "").trim().toUpperCase()) || null;
};

const debtorDaysMissingOrFlag = (value, threshold) => value === null || value >= threshold;
const interestMissingOrFlag = (value, threshold) => value === null || value <= threshold;

module.exports = {
  buildPivotAnalysisRows,
  getPivotAnalysisBuckets,
  getPivotAnalysisBySymbol,
};
