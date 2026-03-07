const { pool } = require("../config/db");
const { toNumber, ensureArray } = require("./common");

const normalizePortfolioType = (row = {}) => ({
  ...row,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  fund: row.fund == null ? null : toNumber(row.fund),
  rules_json: row.rules_json || {},
  important_notes: ensureArray(row.important_notes),
});

const listActive = async (db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM portfolio_type
      WHERE is_active = TRUE
      ORDER BY created_at DESC
    `,
  );

  return rows.map(normalizePortfolioType);
};

const getByIds = async (ids = [], db = pool) => {
  const list = Array.from(new Set(ids.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!list.length) return [];

  const { rows } = await db.query(
    `
      SELECT *
      FROM portfolio_type
      WHERE id = ANY($1::bigint[])
    `,
    [list],
  );

  return rows.map(normalizePortfolioType);
};

const getActiveById = async (id, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM portfolio_type
      WHERE id = $1 AND is_active = TRUE
      LIMIT 1
    `,
    [Number(id)],
  );

  return rows[0] ? normalizePortfolioType(rows[0]) : null;
};

module.exports = {
  normalizePortfolioType,
  listActive,
  getByIds,
  getActiveById,
};
