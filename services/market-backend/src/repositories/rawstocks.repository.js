const { pool } = require("../config/db");
const { toNullableNumber } = require("./common");

const normalizeRawStock = (row = {}) => ({
  ...row,
  exchange: row.exch_seg,
  tick_size: toNullableNumber(row.tick_size),
});

const getById = async (id, db = pool) => {
  const { rows } = await db.query(`SELECT * FROM rawstocks WHERE id = $1 LIMIT 1`, [
    Number(id),
  ]);
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const create = async (data = {}, db = pool) => {
  const token = String(data.token ?? "").trim();
  const symbol = String(data.symbol ?? "").trim();
  const name = String(data.name ?? "").trim();
  const exchSeg = String(data.exchange ?? data.exch_seg ?? "").trim().toUpperCase();
  const instrumentType = String(data.instrumenttype ?? "EQ").trim().toUpperCase() || "EQ";
  const lotsize = Number(data.lotsize ?? data.lot ?? 1);
  const tickSize = toNullableNumber(data.tick_size ?? data.tickSize);

  if (!token || !symbol || !name || !exchSeg) {
    throw new Error("token, symbol, name and exchange are required");
  }

  const { rows } = await db.query(
    `
      INSERT INTO rawstocks (
        token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
      ON CONFLICT (token) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        exch_seg = EXCLUDED.exch_seg,
        instrumenttype = EXCLUDED.instrumenttype,
        lotsize = EXCLUDED.lotsize,
        tick_size = EXCLUDED.tick_size,
        status = 'pending'
      RETURNING *
    `,
    [token, symbol, name, exchSeg, instrumentType, Number.isFinite(lotsize) ? lotsize : 1, tickSize],
  );

  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const updateById = async (id, data = {}, db = pool) => {
  const sets = [];
  const values = [Number(id)];

  Object.entries(data).forEach(([key, value]) => {
    const col = key === "exchange" ? "exch_seg" : key;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });

  if (!sets.length) return getById(id, db);

  const { rows } = await db.query(
    `UPDATE rawstocks SET ${sets.join(", ")} WHERE id = $1 RETURNING *`,
    values,
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const list = async (
  { page = 1, limit = 200, search = "", exchanges = [] } = {},
  db = pool,
) => {
  const offset = (Number(page) - 1) * Number(limit);
  const where = [];
  const values = [];

  if (Array.isArray(exchanges) && exchanges.length) {
    const normalizedExchanges = exchanges.map((x) => String(x).toUpperCase());
    values.push(normalizedExchanges);
    where.push(`upper(exch_seg) = ANY($${values.length})`);
  }

  if (search && search.trim()) {
    values.push(`%${search.trim().toLowerCase()}%`);
    where.push(
      `(lower(symbol) LIKE $${values.length} OR lower(name) LIKE $${values.length} OR lower(token) LIKE $${values.length})`,
    );
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  values.push(Number(limit));
  const limitIdx = values.length;
  values.push(offset);
  const offsetIdx = values.length;
  const whereValuesCount = values.length - 2;

  const dataSql = `
    SELECT *
    FROM rawstocks
    ${whereClause}
    ORDER BY symbol ASC
    LIMIT $${limitIdx} OFFSET $${offsetIdx}
  `;

  const countSql = `SELECT COUNT(*)::int AS total FROM rawstocks ${whereClause}`;

  const [dataRes, countRes] = await Promise.all([
    db.query(dataSql, values),
    db.query(countSql, values.slice(0, whereValuesCount)),
  ]);

  return {
    data: dataRes.rows.map(normalizeRawStock),
    total: Number(countRes.rows[0]?.total || 0),
    page: Number(page),
    limit: Number(limit),
  };
};

module.exports = {
  normalizeRawStock,
  getById,
  create,
  updateById,
  list,
};
