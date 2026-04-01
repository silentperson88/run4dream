const { pool } = require("../config/db");
const { toNumber } = require("./common");

const normalizeActiveStock = (row = {}) => ({
  ...row,
  ltp: toNumber(row.ltp),
  open: toNumber(row.open),
  high: toNumber(row.high),
  low: toNumber(row.low),
  close: toNumber(row.close),
  percentChange: toNumber(row.percent_change),
  avgPrice: toNumber(row.avg_price),
  lowerCircuit: toNumber(row.lower_circuit),
  upperCircuit: toNumber(row.upper_circuit),
  week52Low: toNumber(row.week52_low),
  week52High: toNumber(row.week52_high),
  hasHistoryData: Boolean(row.has_history_data),
  historyDataFromDate: row.history_data_from_date || null,
  historyDataToDate: row.history_data_to_date || null,
  historyRequestedFromDate: row.history_requested_from_date || null,
  historyRequestedToDate: row.history_requested_to_date || null,
});

const create = async (payload, db = pool) => {
  const { rows } = await db.query(
    `
      INSERT INTO active_stock (
        master_id, token, symbol, name, exchange, instrumenttype, is_active,
        ltp, open, high, low, close, percent_change, avg_price,
        lower_circuit, upper_circuit, week52_low, week52_high
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      RETURNING *
    `,
    [
      Number(payload.master_id),
      payload.token,
      payload.symbol,
      payload.name,
      payload.exchange,
      payload.instrumenttype || "EQ",
      payload.is_active ?? true,
      toNumber(payload.ltp, 0),
      toNumber(payload.open, 0),
      toNumber(payload.high, 0),
      toNumber(payload.low, 0),
      toNumber(payload.close, 0),
      toNumber(payload.percent_change ?? payload.percentChange, 0),
      toNumber(payload.avg_price ?? payload.avgPrice, 0),
      toNumber(payload.lower_circuit ?? payload.lowerCircuit, 0),
      toNumber(payload.upper_circuit ?? payload.upperCircuit, 0),
      toNumber(payload.week52_low ?? payload.week52Low, 0),
      toNumber(payload.week52_high ?? payload.week52High, 0),
    ],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByToken = async (token, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE token = $1 LIMIT 1`,
    [token],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const listActive = async ({ page, limit, search = "" } = {}, db = pool) => {
  const queryValues = [];
  const where = ["a.is_active = TRUE"];

  if (search && String(search).trim()) {
    queryValues.push(`%${String(search).trim().toLowerCase()}%`);
    const idx = queryValues.length;
    where.push(
      `(lower(a.name) LIKE $${idx} OR lower(a.symbol) LIKE $${idx} OR lower(a.token) LIKE $${idx})`,
    );
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  if (!page || !limit) {
    const { rows } = await db.query(
      `
        SELECT
          a.*,
          sm.has_history_data,
          sm.history_data_from_date,
          sm.history_data_to_date,
          sm.history_requested_from_date,
          sm.history_requested_to_date
        FROM active_stock a
        LEFT JOIN stock_master sm ON sm.id = a.master_id
        ${whereClause}
        ORDER BY a.added_at DESC
      `,
      queryValues,
    );
    return rows.map(normalizeActiveStock);
  }

  const offset = (Number(page) - 1) * Number(limit);
  queryValues.push(Number(limit));
  const limitIdx = queryValues.length;
  queryValues.push(offset);
  const offsetIdx = queryValues.length;

  const { rows } = await db.query(
    `
      SELECT
        a.*,
        sm.has_history_data,
        sm.history_data_from_date,
        sm.history_data_to_date,
        sm.history_requested_from_date,
        sm.history_requested_to_date
      FROM active_stock a
      LEFT JOIN stock_master sm ON sm.id = a.master_id
      ${whereClause}
      ORDER BY a.added_at DESC
      LIMIT $${limitIdx} OFFSET $${offsetIdx}
    `,
    queryValues,
  );
  return rows.map(normalizeActiveStock);
};

const listByMasterIds = async (masterIds = [], db = pool) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE master_id = ANY($1::bigint[])`,
    [ids],
  );
  return rows.map(normalizeActiveStock);
};

const updateByToken = async (token, data = {}, db = pool) => {
  const map = {
    percentChange: "percent_change",
    avgPrice: "avg_price",
    lowerCircuit: "lower_circuit",
    upperCircuit: "upper_circuit",
    week52Low: "week52_low",
    week52High: "week52_high",
  };

  const sets = [];
  const values = [token];
  Object.entries(data).forEach(([key, value]) => {
    if (key === "updatedAt" || key === "last_update") return;
    const col = map[key] || key;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });

  if (!sets.length) return getByToken(token, db);

  values.push(new Date());
  sets.push(`last_update = $${values.length}`);

  const { rows } = await db.query(
    `UPDATE active_stock SET ${sets.join(", ")} WHERE token = $1 RETURNING *`,
    values,
  );

  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const toggleByToken = async (token, db = pool) => {
  const { rows } = await db.query(
    `
      UPDATE active_stock
      SET is_active = NOT is_active, last_update = NOW()
      WHERE token = $1
      RETURNING *
    `,
    [token],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const deleteByToken = async (token, db = pool) => {
  const { rowCount } = await db.query(`DELETE FROM active_stock WHERE token = $1`, [token]);
  return rowCount > 0;
};

const bulkUpsertByToken = async (stocks = [], fields = [], db = pool) => {
  if (!stocks.length) return;

  for (const stock of stocks) {
    const updates = {};
    fields.forEach((f) => {
      if (stock[f] !== undefined) updates[f] = stock[f];
    });
    updates.last_update = new Date();

    const setCols = Object.keys(updates)
      .map((key, idx) => {
        const map = {
          percentChange: "percent_change",
          avgPrice: "avg_price",
          lowerCircuit: "lower_circuit",
          upperCircuit: "upper_circuit",
          week52Low: "week52_low",
          week52High: "week52_high",
          updatedAt: "last_update",
          last_update: "last_update",
        };
        return `${map[key] || key} = $${idx + 2}`;
      })
      .join(", ");

    const values = [stock.token, ...Object.keys(updates).map((k) => updates[k])];
    await db.query(`UPDATE active_stock SET ${setCols} WHERE token = $1`, values);
  }
};

module.exports = {
  normalizeActiveStock,
  create,
  getByToken,
  getByMasterId,
  listActive,
  listByMasterIds,
  updateByToken,
  toggleByToken,
  deleteByToken,
  bulkUpsertByToken,
};
