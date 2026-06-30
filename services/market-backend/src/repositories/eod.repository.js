let defaultPool = null;

const getDefaultPool = () => {
  if (!defaultPool) {
    ({ pool: defaultPool } = require("../config/db"));
  }
  return defaultPool;
};

function normalizeSymbol(symbol) {
  const base = String(symbol || "").trim().toUpperCase().split("#")[0];
  return base.endsWith("-EQ") ? base.slice(0, -3) : base;
}

const normalizeEod = (row = {}) => ({
  ...row,
  symbol: normalizeSymbol(row.symbol),
});

const DERIVED_EOD_COLUMNS = `
        dma_20,
        dma_50,
        dma_200,
        return_1m,
        return_3m,
        return_6m,
        return_1y,
        week_52_high,
        week_52_low,
        week_52_high_breakout,
        all_time_high,
        all_time_high_breakout,
        avg_volume_20d,
        avg_traded_value_20d,
        traded_days_20d,
        volatility_20d,
        atr_14,
        rsi_14,
        macd_line,
        macd_signal,
        adx_14,
        supertrend_signal,
        is_liquid
`;

const MARKET_SNAPSHOT_COLUMNS = `
        ltp,
        lower_circuit,
        upper_circuit
`;

const upsertDailyCandle = async (doc, db = getDefaultPool()) => {
  const d = new Date(doc.date);
  if (Number.isNaN(d.getTime())) {
    throw new Error("Invalid EOD date");
  }

  const tradeDate = d.toISOString().slice(0, 10);
  const symbol = normalizeSymbol(doc.symbol);

  const { rows } = await db.query(
    `
      INSERT INTO eod (
        master_id,
        symbol,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        ltp,
        volume,
        lower_circuit,
        upper_circuit,
        source,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4::date,
        $5::numeric,
        $6::numeric,
        $7::numeric,
        $8::numeric,
        $9::numeric,
        $10::bigint,
        $11::numeric,
        $12::numeric,
        $13,
        NOW(),
        NOW()
      )
      ON CONFLICT (master_id, trade_date)
      DO UPDATE SET
        symbol = EXCLUDED.symbol,
        exchange = EXCLUDED.exchange,
        open = COALESCE(eod.open, EXCLUDED.open),
        high = GREATEST(eod.high, EXCLUDED.high),
        low = LEAST(eod.low, EXCLUDED.low),
        close = EXCLUDED.close,
        ltp = COALESCE(EXCLUDED.ltp, eod.ltp),
        volume = GREATEST(eod.volume, EXCLUDED.volume),
        lower_circuit = COALESCE(EXCLUDED.lower_circuit, eod.lower_circuit),
        upper_circuit = COALESCE(EXCLUDED.upper_circuit, eod.upper_circuit),
        source = EXCLUDED.source,
        updated_at = NOW()
      RETURNING *
    `,
    [
      Number(doc.master_id),
      symbol,
      doc.exchange || "NSE",
      tradeDate,
      Number(doc.open || 0),
      Number(doc.high || 0),
      Number(doc.low || 0),
      Number(doc.close || 0),
      doc.ltp == null ? null : Number(doc.ltp),
      Number(doc.volume || 0),
      doc.lower_circuit == null ? null : Number(doc.lower_circuit),
      doc.upper_circuit == null ? null : Number(doc.upper_circuit),
      doc.source || "smartapi",
    ],
  );

  return rows[0] ? normalizeEod(rows[0]) : null;
};

const getLatestTradeDateByMasterId = async (masterId, db = getDefaultPool()) => {
  const { rows } = await db.query(
    `
      SELECT MAX(trade_date)::date AS latest_trade_date
      FROM eod
      WHERE master_id = $1
    `,
    [Number(masterId)],
  );

  return rows[0]?.latest_trade_date || null;
};

const getLatestTradeDatesByMasterIds = async (masterIds = [], db = getDefaultPool()) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return new Map();

  const { rows } = await db.query(
    `
      SELECT master_id, MAX(trade_date)::date AS latest_trade_date
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      GROUP BY master_id
    `,
    [ids],
  );

  return new Map(rows.map((row) => [Number(row.master_id), row.latest_trade_date || null]));
};

const getLatestTradeDatesByMasterIdsAsOfDate = async (masterIds = [], asOfDate, db = getDefaultPool()) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return new Map();

  const values = [ids];
  let dateClause = "";
  if (asOfDate) {
    values.push(asOfDate);
    dateClause = `AND trade_date <= $${values.length}::date`;
  }

  const { rows } = await db.query(
    `
      SELECT master_id, MAX(trade_date)::date AS latest_trade_date
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      ${dateClause}
      GROUP BY master_id
    `,
    values,
  );

  return new Map(rows.map((row) => [Number(row.master_id), row.latest_trade_date || null]));
};

const getLatestCandleRowsByMasterIds = async (masterIds = [], asOfDate = null, db = getDefaultPool()) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const values = [ids];
  let dateClause = "";
  if (asOfDate) {
    values.push(asOfDate);
    dateClause = `AND trade_date <= $${values.length}::date`;
  }

  const { rows } = await db.query(
    `
      SELECT DISTINCT ON (master_id)
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
${MARKET_SNAPSHOT_COLUMNS},
        source,
 ${DERIVED_EOD_COLUMNS}
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      ${dateClause}
      ORDER BY master_id ASC, trade_date DESC
    `,
    values,
  );

  return rows.map(normalizeEod);
};

const getLatestCandleSnapshotsByMasterIds = async (masterIds = [], asOfDate = null, db = getDefaultPool()) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const values = [ids];
  let dateClause = "";
  if (asOfDate) {
    values.push(asOfDate);
    dateClause = `AND trade_date <= $${values.length}::date`;
  }

  const { rows } = await db.query(
    `
      SELECT DISTINCT ON (master_id)
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        ltp,
        volume,
        lower_circuit,
        upper_circuit,
        source,
        dma_20,
        dma_50,
        dma_200,
        return_1m,
        return_3m,
        return_6m,
        return_1y,
        week_52_high,
        week_52_low,
        week_52_high_breakout,
        all_time_high,
        all_time_high_breakout,
        avg_volume_20d,
        avg_traded_value_20d,
        traded_days_20d,
        volatility_20d,
        atr_14,
        rsi_14,
        macd_line,
        macd_signal,
        adx_14,
        supertrend_signal,
        is_liquid
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      ${dateClause}
      ORDER BY master_id ASC, trade_date DESC
    `,
    values,
  );

  return rows.map(normalizeEod);
};

const listRecentCandlesByMasterIds = async (
  masterIds = [],
  { limitPerMaster = 260, asOfDate = null } = {},
  db = getDefaultPool(),
) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const safeLimit = Math.max(1, Math.min(1000, Number(limitPerMaster) || 260));
  const values = [ids];
  let dateClause = "";
  if (asOfDate) {
    values.push(asOfDate);
    dateClause = `AND trade_date <= $${values.length}::date`;
  }
  values.push(safeLimit);

  const { rows } = await db.query(
    `
      WITH ranked AS (
        SELECT
          master_id,
          trade_date,
          open,
          high,
          low,
          close,
          volume,
${MARKET_SNAPSHOT_COLUMNS},
          source,
 ${DERIVED_EOD_COLUMNS}
          , ROW_NUMBER() OVER (PARTITION BY master_id ORDER BY trade_date DESC) AS rn
        FROM eod
        WHERE master_id = ANY($1::bigint[])
        ${dateClause}
      )
      SELECT
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
${MARKET_SNAPSHOT_COLUMNS},
        source,
 ${DERIVED_EOD_COLUMNS}
      FROM ranked
      WHERE rn <= $${values.length}
      ORDER BY master_id ASC, trade_date ASC
    `,
    values,
  );

  return rows.map(normalizeEod);
};

const listRecentCandleSnapshotsByMasterIds = async (
  masterIds = [],
  { limitPerMaster = 260, asOfDate = null } = {},
  db = getDefaultPool(),
) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const safeLimit = Math.max(1, Math.min(1000, Number(limitPerMaster) || 260));
  const values = [ids];
  let dateClause = "";
  if (asOfDate) {
    values.push(asOfDate);
    dateClause = `AND trade_date <= $${values.length}::date`;
  }
  values.push(safeLimit);

  const { rows } = await db.query(
    `
      WITH ranked AS (
        SELECT
          master_id,
          trade_date,
          open,
          high,
          low,
          close,
          ltp,
          volume,
          lower_circuit,
          upper_circuit,
          source,
          dma_20,
          dma_50,
          dma_200,
          return_1m,
          return_3m,
          return_6m,
          return_1y,
          week_52_high,
          week_52_low,
          week_52_high_breakout,
          all_time_high,
          all_time_high_breakout,
          avg_volume_20d,
          avg_traded_value_20d,
          traded_days_20d,
          volatility_20d,
          atr_14,
          rsi_14,
          macd_line,
          macd_signal,
          adx_14,
          supertrend_signal,
          is_liquid,
          ROW_NUMBER() OVER (PARTITION BY master_id ORDER BY trade_date DESC) AS rn
        FROM eod
        WHERE master_id = ANY($1::bigint[])
        ${dateClause}
      )
      SELECT
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        ltp,
        volume,
        lower_circuit,
        upper_circuit,
        source,
        dma_20,
        dma_50,
        dma_200,
        return_1m,
        return_3m,
        return_6m,
        return_1y,
        week_52_high,
        week_52_low,
        week_52_high_breakout,
        all_time_high,
        all_time_high_breakout,
        avg_volume_20d,
        avg_traded_value_20d,
        traded_days_20d,
        volatility_20d,
        atr_14,
        rsi_14,
        macd_line,
        macd_signal,
        adx_14,
        supertrend_signal,
        is_liquid
      FROM ranked
      WHERE rn <= $${values.length}
      ORDER BY master_id ASC, trade_date ASC
    `,
    values,
  );

  return rows.map(normalizeEod);
};

const listDailyCandlesByMasterIdRange = async (
  { master_id, fromDate, toDate, limit = 5000 } = {},
  db = getDefaultPool(),
) => {
  const values = [Number(master_id)];
  const where = ["master_id = $1"];

  if (fromDate) {
    values.push(fromDate);
    where.push(`trade_date >= $${values.length}::date`);
  }

  if (toDate) {
    values.push(toDate);
    where.push(`trade_date <= $${values.length}::date`);
  }

  values.push(Number(limit));
  const limitIdx = values.length;

  const { rows } = await db.query(
    `
      SELECT
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
${MARKET_SNAPSHOT_COLUMNS},
        source,
 ${DERIVED_EOD_COLUMNS}
      FROM eod
      WHERE ${where.join(" AND ")}
      ORDER BY trade_date ASC
      LIMIT $${limitIdx}
    `,
    values,
  );

  return rows.map(normalizeEod);
};

const listMasterIdsForDerivedMetrics = async (
  {
    afterMasterId = 0,
    limit = 25,
    shardCount = 1,
    shardIndex = 0,
    masterId = null,
  } = {},
  db = getDefaultPool(),
) => {
  if (masterId) return [Number(masterId)];

  const safeShardCount = Math.max(1, Number(shardCount) || 1);
  const safeShardIndex = Math.max(0, Number(shardIndex) || 0);
  const safeLimit = Math.max(1, Math.min(500, Number(limit) || 25));
  const params = [Number(afterMasterId) || 0, safeShardCount, safeShardIndex, safeLimit];

  const { rows } = await db.query(
    `
      SELECT master_id
      FROM eod
      WHERE master_id > $1
        AND MOD(master_id, $2::bigint) = $3::bigint
      GROUP BY master_id
      ORDER BY master_id ASC
      LIMIT $4
    `,
    params,
  );

  return rows.map((row) => Number(row.master_id)).filter((value) => Number.isFinite(value) && value > 0);
};

const listAllCandlesByMasterIds = async (masterIds = [], db = getDefaultPool()) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const { rows } = await db.query(
    `
      SELECT
        master_id,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
${MARKET_SNAPSHOT_COLUMNS},
        source,
 ${DERIVED_EOD_COLUMNS}
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id ASC, trade_date ASC
    `,
    [ids],
  );

  return rows.map(normalizeEod);
};

const bulkUpdateDerivedMetrics = async (rows = [], db = getDefaultPool()) => {
  if (!rows.length) return 0;

  const payload = JSON.stringify(rows);
  const { rowCount } = await db.query(
    `
      WITH staged AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS x(
          master_id BIGINT,
          trade_date DATE,
          dma_20 NUMERIC,
          dma_50 NUMERIC,
          dma_200 NUMERIC,
          return_1m NUMERIC,
          return_3m NUMERIC,
          return_6m NUMERIC,
          return_1y NUMERIC,
          week_52_high NUMERIC,
          week_52_low NUMERIC,
          week_52_high_breakout BOOLEAN,
          all_time_high NUMERIC,
          all_time_high_breakout BOOLEAN,
          avg_volume_20d INTEGER,
          avg_traded_value_20d BIGINT,
          traded_days_20d SMALLINT,
          volatility_20d NUMERIC,
          atr_14 NUMERIC,
          rsi_14 NUMERIC,
          macd_line NUMERIC,
          macd_signal NUMERIC,
          adx_14 NUMERIC,
          supertrend_signal SMALLINT,
          is_liquid BOOLEAN
        )
      )
      UPDATE eod AS target
      SET
        dma_20 = staged.dma_20,
        dma_50 = staged.dma_50,
        dma_200 = staged.dma_200,
        return_1m = staged.return_1m,
        return_3m = staged.return_3m,
        return_6m = staged.return_6m,
        return_1y = staged.return_1y,
        week_52_high = staged.week_52_high,
        week_52_low = staged.week_52_low,
        week_52_high_breakout = staged.week_52_high_breakout,
        all_time_high = staged.all_time_high,
        all_time_high_breakout = staged.all_time_high_breakout,
        avg_volume_20d = staged.avg_volume_20d,
        avg_traded_value_20d = staged.avg_traded_value_20d,
        traded_days_20d = staged.traded_days_20d,
        volatility_20d = staged.volatility_20d,
        atr_14 = staged.atr_14,
        rsi_14 = staged.rsi_14,
        macd_line = staged.macd_line,
        macd_signal = staged.macd_signal,
        adx_14 = staged.adx_14,
        supertrend_signal = staged.supertrend_signal,
        is_liquid = staged.is_liquid
      FROM staged
      WHERE target.master_id = staged.master_id
        AND target.trade_date = staged.trade_date
    `,
    [payload],
  );

  return rowCount || 0;
};

module.exports = {
  normalizeEod,
  normalizeSymbol,
  upsertDailyCandle,
  getLatestTradeDateByMasterId,
  getLatestTradeDatesByMasterIds,
  getLatestTradeDatesByMasterIdsAsOfDate,
  getLatestCandleRowsByMasterIds,
  getLatestCandleSnapshotsByMasterIds,
  listRecentCandlesByMasterIds,
  listRecentCandleSnapshotsByMasterIds,
  listDailyCandlesByMasterIdRange,
  listMasterIdsForDerivedMetrics,
  listAllCandlesByMasterIds,
  bulkUpdateDerivedMetrics,
  upsertMonthlyCandle: upsertDailyCandle,
};


