const { pool } = require("../config/db");

const normalizePlan = (row = {}) => ({
  ...row,
  buy_lots: Array.isArray(row.buy_lots) ? row.buy_lots : row.buy_lots || [],
  sell_trades: Array.isArray(row.sell_trades) ? row.sell_trades : row.sell_trades || [],
  settings: row.settings || {},
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const listByUser = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM tax_planner_plans
      WHERE user_id = $1
      ORDER BY updated_at DESC, id DESC
    `,
    [Number(userId)],
  );

  return rows.map(normalizePlan);
};

const getLatestByUser = async (userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM tax_planner_plans
      WHERE user_id = $1
      ORDER BY updated_at DESC, id DESC
      LIMIT 1
    `,
    [Number(userId)],
  );

  return rows[0] ? normalizePlan(rows[0]) : null;
};

const getById = async (planId, userId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM tax_planner_plans
      WHERE id = $1 AND user_id = $2
      LIMIT 1
    `,
    [Number(planId), Number(userId)],
  );

  return rows[0] ? normalizePlan(rows[0]) : null;
};

const upsertByName = async (
  { user_id, name, buy_lots, sell_trades, settings },
  db = pool,
) => {
  const planName = String(name || "Default").trim() || "Default";
  const buyLotsJson = JSON.stringify(Array.isArray(buy_lots) ? buy_lots : []);
  const sellTradesJson = JSON.stringify(Array.isArray(sell_trades) ? sell_trades : []);
  const settingsJson = JSON.stringify(settings && typeof settings === "object" ? settings : {});

  const { rows } = await db.query(
    `
      INSERT INTO tax_planner_plans (
        user_id, name, buy_lots, sell_trades, settings, updated_at
      )
      VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, NOW())
      ON CONFLICT (user_id, name)
      DO UPDATE SET
        buy_lots = EXCLUDED.buy_lots,
        sell_trades = EXCLUDED.sell_trades,
        settings = EXCLUDED.settings,
        updated_at = NOW()
      RETURNING *
    `,
    [Number(user_id), planName, buyLotsJson, sellTradesJson, settingsJson],
  );

  return rows[0] ? normalizePlan(rows[0]) : null;
};

const updateById = async ({ plan_id, user_id, name, buy_lots, sell_trades, settings }, db = pool) => {
  const sets = [];
  const values = [Number(plan_id), Number(user_id)];

  if (name !== undefined) {
    values.push(String(name || "Default").trim() || "Default");
    sets.push(`name = $${values.length}`);
  }

  if (buy_lots !== undefined) {
    values.push(JSON.stringify(Array.isArray(buy_lots) ? buy_lots : []));
    sets.push(`buy_lots = $${values.length}::jsonb`);
  }

  if (sell_trades !== undefined) {
    values.push(JSON.stringify(Array.isArray(sell_trades) ? sell_trades : []));
    sets.push(`sell_trades = $${values.length}::jsonb`);
  }

  if (settings !== undefined) {
    values.push(JSON.stringify(settings && typeof settings === "object" ? settings : {}));
    sets.push(`settings = $${values.length}::jsonb`);
  }

  if (!sets.length) {
    return getById(plan_id, user_id, db);
  }

  sets.push("updated_at = NOW()");

  const { rows } = await db.query(
    `
      UPDATE tax_planner_plans
      SET ${sets.join(", ")}
      WHERE id = $1 AND user_id = $2
      RETURNING *
    `,
    values,
  );

  return rows[0] ? normalizePlan(rows[0]) : null;
};

module.exports = {
  normalizePlan,
  listByUser,
  getLatestByUser,
  getById,
  upsertByName,
  updateById,
};
