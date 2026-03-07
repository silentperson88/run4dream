const { pool } = require("../config/db");
const { toNumber } = require("./common");

const normalizeActiveStock = (row = {}) => ({
  ...row,
  ltp: toNumber(row.ltp),
  lower_circuit: toNumber(row.lower_circuit),
  upper_circuit: toNumber(row.upper_circuit),
});

const getById = async (stockId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM active_stock
      WHERE id = $1
      LIMIT 1
    `,
    [Number(stockId)],
  );

  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByIds = async (ids = [], db = pool) => {
  const list = Array.from(new Set(ids.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!list.length) return [];

  const { rows } = await db.query(
    `
      SELECT *
      FROM active_stock
      WHERE id = ANY($1::bigint[])
    `,
    [list],
  );

  return rows.map(normalizeActiveStock);
};

const getBySymbols = async (symbols = [], db = pool) => {
  const list = Array.from(new Set(symbols.filter(Boolean)));
  if (!list.length) return [];

  const { rows } = await db.query(
    `
      SELECT *
      FROM active_stock
      WHERE symbol = ANY($1::text[])
    `,
    [list],
  );

  return rows.map(normalizeActiveStock);
};

module.exports = {
  normalizeActiveStock,
  getById,
  getByIds,
  getBySymbols,
};
