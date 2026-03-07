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
  updateById,
  list,
};
