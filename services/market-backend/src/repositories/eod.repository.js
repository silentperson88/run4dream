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

module.exports = {
  normalizeEod,
  normalizeSymbol,
  upsertDailyCandle,
  getLatestTradeDateByMasterId,
  getLatestTradeDatesByMasterIds,
  listDailyCandlesByMasterIdRange,
  upsertMonthlyCandle: upsertDailyCandle,
};
