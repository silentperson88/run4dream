const { pool } = require("../config/db");
const { syncSerialSequence } = require("./common");

const normalizeToken = (row = {}) => ({
  ...row,
  feedToken: row.feed_token,
  market: {
    open_time: row.market_open_time,
    close_time: row.market_close_time,
  },
  scheduler: {
    state: row.scheduler_state,
    current_phase: row.scheduler_current_phase,
  },
});

const create = async (payload, db = pool) => {
  await syncSerialSequence(db, "tokens", "id");
  const { rows } = await db.query(
    `
      INSERT INTO tokens (
        totp, access_token, refresh_token, feed_token,
        expiry_time, generated_by, is_active,
        market_open_time, market_close_time,
        scheduler_state, scheduler_current_phase,
        generated_at, updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),NOW())
      RETURNING *
    `,
    [
      payload.totp,
      payload.access_token,
      payload.refresh_token,
      payload.feedToken || payload.feed_token || null,
      payload.expiry_time,
      payload.generated_by || "api",
      payload.is_active ?? true,
      payload.market?.open_time || payload.market_open_time || "09:15",
      payload.market?.close_time || payload.market_close_time || "15:30",
      payload.scheduler?.state || payload.scheduler_state || "NOT_STARTED",
      payload.scheduler?.current_phase || payload.scheduler_current_phase || "NONE",
    ],
  );
  return rows[0] ? normalizeToken(rows[0]) : null;
};

const getLastEntry = async (db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM tokens ORDER BY generated_at DESC LIMIT 1`,
  );
  return rows[0] ? normalizeToken(rows[0]) : null;
};

const updateById = async (id, data, db = pool) => {
  const sets = [];
  const values = [Number(id)];
  const add = (col, val) => {
    values.push(val);
    sets.push(`${col} = $${values.length}`);
  };

  if (data.access_token !== undefined) add("access_token", data.access_token);
  if (data.refresh_token !== undefined) add("refresh_token", data.refresh_token);
  if (data.feedToken !== undefined || data.feed_token !== undefined)
    add("feed_token", data.feedToken ?? data.feed_token);
  if (data.expiry_time !== undefined) add("expiry_time", data.expiry_time);
  if (data.totp !== undefined) add("totp", data.totp);
  if (data.is_active !== undefined) add("is_active", data.is_active);
  if (data.market?.open_time !== undefined || data.market_open_time !== undefined)
    add("market_open_time", data.market?.open_time ?? data.market_open_time);
  if (data.market?.close_time !== undefined || data.market_close_time !== undefined)
    add("market_close_time", data.market?.close_time ?? data.market_close_time);
  if (data.scheduler?.state !== undefined || data.scheduler_state !== undefined)
    add("scheduler_state", data.scheduler?.state ?? data.scheduler_state);
  if (data.scheduler?.current_phase !== undefined || data.scheduler_current_phase !== undefined)
    add(
      "scheduler_current_phase",
      data.scheduler?.current_phase ?? data.scheduler_current_phase,
    );

  if (!sets.length) return getById(id, db);
  sets.push("updated_at = NOW()");

  const { rows } = await db.query(
    `UPDATE tokens SET ${sets.join(", ")} WHERE id = $1 RETURNING *`,
    values,
  );
  return rows[0] ? normalizeToken(rows[0]) : null;
};

const getById = async (id, db = pool) => {
  const { rows } = await db.query(`SELECT * FROM tokens WHERE id = $1 LIMIT 1`, [
    Number(id),
  ]);
  return rows[0] ? normalizeToken(rows[0]) : null;
};

const inactivateById = async (id, db = pool) =>
  updateById(id, { is_active: false }, db);

module.exports = {
  normalizeToken,
  create,
  getLastEntry,
  getById,
  updateById,
  inactivateById,
};
