const { pool } = require("../config/db");
const { toNumber, ensureArray } = require("./common");

const normalizePortfolio = (row = {}) => ({
  ...row,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  initial_fund: toNumber(row.initial_fund),
  available_fund: toNumber(row.available_fund),
  holdings: ensureArray(row.holdings),
  lock_fund: ensureArray(row.lock_fund),
});

const getActiveById = async (portfolioId, userId, db = pool, { forUpdate = false } = {}) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM user_portfolios
      WHERE id = $1 AND user_id = $2 AND status = 'ACTIVE'
      LIMIT 1
      ${forUpdate ? "FOR UPDATE" : ""}
    `,
    [Number(portfolioId), Number(userId)],
  );

  return rows[0] ? normalizePortfolio(rows[0]) : null;
};

const getActiveByIdAnyUser = async (portfolioId, db = pool, { forUpdate = false } = {}) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM user_portfolios
      WHERE id = $1 AND status = 'ACTIVE'
      LIMIT 1
      ${forUpdate ? "FOR UPDATE" : ""}
    `,
    [Number(portfolioId)],
  );

  return rows[0] ? normalizePortfolio(rows[0]) : null;
};

const findActiveByUserAndName = async (userId, name, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM user_portfolios
      WHERE user_id = $1 AND name = $2 AND status = 'ACTIVE'
      LIMIT 1
    `,
    [Number(userId), name],
  );

  return rows[0] ? normalizePortfolio(rows[0]) : null;
};

const create = async ({ user_id, portfolio_type_id, name, initial_fund, available_fund }, db = pool) => {
  const { rows } = await db.query(
    `
      INSERT INTO user_portfolios (
        user_id, portfolio_type_id, name, initial_fund, available_fund,
        status, holdings, lock_fund
      )
      VALUES ($1, $2, $3, $4, $5, 'ACTIVE', '[]'::jsonb, '[]'::jsonb)
      RETURNING *
    `,
    [
      Number(user_id),
      Number(portfolio_type_id),
      name,
      toNumber(initial_fund),
      toNumber(available_fund),
    ],
  );

  return normalizePortfolio(rows[0]);
};

const listActiveByUser = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM user_portfolios
      WHERE user_id = $1 AND status = 'ACTIVE'
      ORDER BY created_at DESC
    `,
    [Number(userId)],
  );

  return rows.map(normalizePortfolio);
};

const updateFinancialState = async (
  portfolioId,
  { available_fund, holdings, lock_fund, initial_fund },
  db = pool,
) => {
  const values = [Number(portfolioId)];
  const sets = [];

  if (available_fund !== undefined) {
    values.push(toNumber(available_fund));
    sets.push(`available_fund = $${values.length}`);
  }

  if (holdings !== undefined) {
    values.push(JSON.stringify(ensureArray(holdings)));
    sets.push(`holdings = $${values.length}::jsonb`);
  }

  if (lock_fund !== undefined) {
    values.push(JSON.stringify(ensureArray(lock_fund)));
    sets.push(`lock_fund = $${values.length}::jsonb`);
  }

  if (initial_fund !== undefined) {
    values.push(toNumber(initial_fund));
    sets.push(`initial_fund = $${values.length}`);
  }

  if (!sets.length) {
    const unchanged = await getActiveByIdAnyUser(portfolioId, db);
    return unchanged;
  }

  sets.push("updated_at = NOW()");

  const { rows } = await db.query(
    `
      UPDATE user_portfolios
      SET ${sets.join(", ")}
      WHERE id = $1
      RETURNING *
    `,
    values,
  );

  return rows[0] ? normalizePortfolio(rows[0]) : null;
};

const archiveByIdAndUser = async (portfolioId, userId, db = pool) => {
  const { rowCount } = await db.query(
    `
      UPDATE user_portfolios
      SET status = 'ARCHIVED', updated_at = NOW()
      WHERE id = $1 AND user_id = $2 AND status = 'ACTIVE'
    `,
    [Number(portfolioId), Number(userId)],
  );

  return rowCount > 0;
};

module.exports = {
  normalizePortfolio,
  getActiveById,
  getActiveByIdAnyUser,
  findActiveByUserAndName,
  create,
  listActiveByUser,
  updateFinancialState,
  archiveByIdAndUser,
};
