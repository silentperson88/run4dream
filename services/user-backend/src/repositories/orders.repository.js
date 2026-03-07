const { pool } = require("../config/db");
const { toNumber, ensureArray } = require("./common");

const normalizeOrder = (row = {}) => ({
  ...row,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  order_price: toNumber(row.order_price),
  avg_execution_price: toNumber(row.avg_execution_price),
  realized_pl: toNumber(row.realized_pl),
  executions: ensureArray(row.executions),
  sell_allocation: ensureArray(row.sell_allocation),
});

const create = async (payload, db = pool) => {
  const { rows } = await db.query(
    `
      INSERT INTO orders (
        user_id, portfolio_id, active_stock_id, symbol, exchange,
        type, order_type, order_price, order_quantity, executed_quantity,
        remaining_quantity, avg_execution_price, status, executions, realized_pl
      )
      VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        $11, $12, $13, $14::jsonb, $15
      )
      RETURNING *
    `,
    [
      Number(payload.user_id),
      Number(payload.portfolio_id),
      Number(payload.active_stock_id),
      payload.symbol,
      payload.exchange,
      payload.type,
      payload.order_type,
      toNumber(payload.order_price),
      Number(payload.order_quantity),
      Number(payload.executed_quantity),
      Number(payload.remaining_quantity),
      toNumber(payload.avg_execution_price),
      payload.status,
      JSON.stringify(ensureArray(payload.executions)),
      toNumber(payload.realized_pl),
    ],
  );

  return normalizeOrder(rows[0]);
};

const updateExecutionState = async (
  orderId,
  { executed_quantity, remaining_quantity, executions, realized_pl, status },
  db = pool,
) => {
  const { rows } = await db.query(
    `
      UPDATE orders
      SET
        executed_quantity = $1,
        remaining_quantity = $2,
        executions = $3::jsonb,
        realized_pl = $4,
        status = $5,
        updated_at = NOW()
      WHERE id = $6
      RETURNING *
    `,
    [
      Number(executed_quantity),
      Number(remaining_quantity),
      JSON.stringify(ensureArray(executions)),
      toNumber(realized_pl),
      status,
      Number(orderId),
    ],
  );

  return rows[0] ? normalizeOrder(rows[0]) : null;
};

const updateStatus = async (orderId, status, db = pool) => {
  const { rows } = await db.query(
    `
      UPDATE orders
      SET status = $1, updated_at = NOW()
      WHERE id = $2
      RETURNING *
    `,
    [status, Number(orderId)],
  );

  return rows[0] ? normalizeOrder(rows[0]) : null;
};

const listOpenByUser = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1
        AND status IN ('OPEN', 'PARTIALLY_FILLED')
      ORDER BY created_at DESC
    `,
    [Number(userId)],
  );

  return rows.map(normalizeOrder);
};

const listOpenByPortfolio = async (userId, portfolioId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1
        AND portfolio_id = $2
        AND status IN ('OPEN', 'PARTIALLY_FILLED')
      ORDER BY created_at DESC
    `,
    [Number(userId), Number(portfolioId)],
  );

  return rows.map(normalizeOrder);
};

const listByUserPortfolio = async (userId, portfolioId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1 AND portfolio_id = $2
      ORDER BY created_at DESC
    `,
    [Number(userId), Number(portfolioId)],
  );

  return rows.map(normalizeOrder);
};

const listByUserPortfolioStock = async (userId, portfolioId, stockId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1 AND portfolio_id = $2 AND active_stock_id = $3
      ORDER BY created_at DESC
    `,
    [Number(userId), Number(portfolioId), Number(stockId)],
  );

  return rows.map(normalizeOrder);
};

const listSellExecutedByUser = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1 AND type = 'SELL' AND executed_quantity > 0
    `,
    [Number(userId)],
  );

  return rows.map(normalizeOrder);
};

const listSellExecutedByUserFromDate = async (userId, fromIsoDate, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1
        AND type = 'SELL'
        AND executed_quantity > 0
        AND created_at >= $2
    `,
    [Number(userId), fromIsoDate],
  );

  return rows.map(normalizeOrder);
};

const listBuyExecutedByUserFromDate = async (userId, fromIsoDate, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1
        AND type = 'BUY'
        AND executed_quantity > 0
        AND created_at >= $2
    `,
    [Number(userId), fromIsoDate],
  );

  return rows.map(normalizeOrder);
};

const sumRealizedPlByUserPortfolio = async (userId, portfolioId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT COALESCE(SUM(realized_pl), 0) AS total
      FROM orders
      WHERE
        user_id = $1
        AND portfolio_id = $2
        AND type = 'SELL'
        AND executed_quantity > 0
    `,
    [Number(userId), Number(portfolioId)],
  );

  return toNumber(rows[0]?.total);
};

const listLatestByUser = async (userId, limit = 20, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE user_id = $1
      ORDER BY created_at DESC
      LIMIT $2
    `,
    [Number(userId), Number(limit)],
  );

  return rows.map(normalizeOrder);
};

const countByStatusAndType = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT status, type, COUNT(*)::int AS count
      FROM orders
      WHERE user_id = $1
      GROUP BY status, type
    `,
    [Number(userId)],
  );
  return rows;
};

const listOpenForExecution = async (db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM orders
      WHERE status IN ('OPEN', 'PARTIALLY_FILLED')
      ORDER BY created_at ASC
      FOR UPDATE
    `,
  );

  return rows.map(normalizeOrder);
};

module.exports = {
  normalizeOrder,
  create,
  updateExecutionState,
  updateStatus,
  listOpenByUser,
  listOpenByPortfolio,
  listByUserPortfolio,
  listByUserPortfolioStock,
  listSellExecutedByUser,
  listSellExecutedByUserFromDate,
  listBuyExecutedByUserFromDate,
  sumRealizedPlByUserPortfolio,
  listLatestByUser,
  countByStatusAndType,
  listOpenForExecution,
};
