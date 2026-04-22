const { pool } = require("../config/db");

function normalizeSymbol(symbol) {
  const base = String(symbol || "").trim().toUpperCase().split("#")[0];
  return base.endsWith("-EQ") ? base.slice(0, -3) : base;
}

const normalizeEod = (row = {}) => ({
  ...row,
  symbol: normalizeSymbol(row.symbol),
});

const upsertDailyCandle = async (doc, db = pool) => {
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
        volume,
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
        $9::bigint,
        $10,
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
        volume = GREATEST(eod.volume, EXCLUDED.volume),
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
      Number(doc.volume || 0),
      doc.source || "smartapi",
    ],
  );

  return rows[0] ? normalizeEod(rows[0]) : null;
};

const getLatestTradeDateByMasterId = async (masterId, db = pool) => {
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

const getLatestTradeDatesByMasterIds = async (masterIds = [], db = pool) => {
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

const getLatestTradeDatesByMasterIdsAsOfDate = async (masterIds = [], asOfDate, db = pool) => {
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

const getLatestCandleRowsByMasterIds = async (masterIds = [], asOfDate = null, db = pool) => {
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
        symbol,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        source
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
  db = pool,
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
          symbol,
          exchange,
          trade_date,
          open,
          high,
          low,
          close,
          volume,
        source,
          ROW_NUMBER() OVER (PARTITION BY master_id ORDER BY trade_date DESC) AS rn
        FROM eod
        WHERE master_id = ANY($1::bigint[])
        ${dateClause}
      )
      SELECT
        master_id,
        symbol,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        source
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
  db = pool,
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
        symbol,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        source
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
  db = pool,
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

const listAllCandlesByMasterIds = async (masterIds = [], db = pool) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];

  const { rows } = await db.query(
    `
      SELECT
        master_id,
        symbol,
        exchange,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        source
      FROM eod
      WHERE master_id = ANY($1::bigint[])
      ORDER BY master_id ASC, trade_date ASC
    `,
    [ids],
  );

  return rows.map(normalizeEod);
};

const bulkUpdateDerivedMetrics = async (rows = [], db = pool) => {
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
          dma_50_slope NUMERIC,
          dma_200_slope NUMERIC,
          price_vs_dma_50_pct NUMERIC,
          price_vs_dma_200_pct NUMERIC,
          dma_50_vs_dma_200 NUMERIC,
          return_1w NUMERIC,
          return_1m NUMERIC,
          return_3m NUMERIC,
          return_6m NUMERIC,
          return_1y NUMERIC,
          week_52_high NUMERIC,
          week_52_low NUMERIC,
          distance_from_52w_high_pct NUMERIC,
          distance_from_52w_low_pct NUMERIC,
          near_52w_high BOOLEAN,
          week_52_high_breakout BOOLEAN,
          all_time_high NUMERIC,
          distance_from_ath_pct NUMERIC,
          all_time_high_breakout BOOLEAN,
          avg_volume_20d NUMERIC,
          avg_traded_value_20d NUMERIC,
          avg_traded_value_50d NUMERIC,
          volume_ratio NUMERIC,
          traded_days_20d INTEGER,
          volatility_20d NUMERIC,
          volatility_50d NUMERIC,
          atr_14 NUMERIC,
          atr_pct NUMERIC,
          rsi_14 NUMERIC,
          macd_line NUMERIC,
          macd_signal NUMERIC,
          macd_histogram NUMERIC,
          adx_14 NUMERIC,
          supertrend NUMERIC,
          supertrend_signal SMALLINT,
          higher_high_20d BOOLEAN,
          higher_low_20d BOOLEAN,
          is_liquid BOOLEAN,
          derived_meta JSONB
        )
      )
      UPDATE eod AS target
      SET
        dma_20 = staged.dma_20,
        dma_50 = staged.dma_50,
        dma_200 = staged.dma_200,
        dma_50_slope = staged.dma_50_slope,
        dma_200_slope = staged.dma_200_slope,
        price_vs_dma_50_pct = staged.price_vs_dma_50_pct,
        price_vs_dma_200_pct = staged.price_vs_dma_200_pct,
        dma_50_vs_dma_200 = staged.dma_50_vs_dma_200,
        return_1w = staged.return_1w,
        return_1m = staged.return_1m,
        return_3m = staged.return_3m,
        return_6m = staged.return_6m,
        return_1y = staged.return_1y,
        week_52_high = staged.week_52_high,
        week_52_low = staged.week_52_low,
        distance_from_52w_high_pct = staged.distance_from_52w_high_pct,
        distance_from_52w_low_pct = staged.distance_from_52w_low_pct,
        near_52w_high = staged.near_52w_high,
        week_52_high_breakout = staged.week_52_high_breakout,
        all_time_high = staged.all_time_high,
        distance_from_ath_pct = staged.distance_from_ath_pct,
        all_time_high_breakout = staged.all_time_high_breakout,
        avg_volume_20d = staged.avg_volume_20d,
        avg_traded_value_20d = staged.avg_traded_value_20d,
        avg_traded_value_50d = staged.avg_traded_value_50d,
        volume_ratio = staged.volume_ratio,
        traded_days_20d = staged.traded_days_20d,
        volatility_20d = staged.volatility_20d,
        volatility_50d = staged.volatility_50d,
        atr_14 = staged.atr_14,
        atr_pct = staged.atr_pct,
        rsi_14 = staged.rsi_14,
        macd_line = staged.macd_line,
        macd_signal = staged.macd_signal,
        macd_histogram = staged.macd_histogram,
        adx_14 = staged.adx_14,
        supertrend = staged.supertrend,
        supertrend_signal = staged.supertrend_signal,
        higher_high_20d = staged.higher_high_20d,
        higher_low_20d = staged.higher_low_20d,
        is_liquid = staged.is_liquid,
        derived_meta = staged.derived_meta
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
  listRecentCandlesByMasterIds,
  listDailyCandlesByMasterIdRange,
  listMasterIdsForDerivedMetrics,
  listAllCandlesByMasterIds,
  bulkUpdateDerivedMetrics,
  upsertMonthlyCandle: upsertDailyCandle,
};
