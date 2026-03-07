const { pool } = require("../config/db");
const { toNumber, ensureArray } = require("./common");

const normalizeUser = (row = {}) => ({
  ...row,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  password: row.password_hash,
  isActive: !!row.is_active,
  isEmailVerified: !!row.is_email_verified,
  emailVerifiedAt: row.email_verified_at,
  wallet_fund: toNumber(row.wallet_fund),
  total_fund_added: toNumber(row.total_fund_added),
  total_fund_withdrawn: toNumber(row.total_fund_withdrawn),
  wallet_ledger: ensureArray(row.wallet_ledger),
  email_otp: {
    code_hash: row.email_otp_code_hash || null,
    purpose: row.email_otp_purpose || null,
    expires_at: row.email_otp_expires_at || null,
    last_sent_at: row.email_otp_last_sent_at || null,
  },
});

const getById = async (userId, db = pool, { forUpdate = false } = {}) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM users
      WHERE id = $1
      LIMIT 1
      ${forUpdate ? "FOR UPDATE" : ""}
    `,
    [Number(userId)],
  );

  return rows[0] ? normalizeUser(rows[0]) : null;
};

const getByEmail = async (email, db = pool, { forUpdate = false, columns = "*" } = {}) => {
  const { rows } = await db.query(
    `
      SELECT ${columns}
      FROM users
      WHERE email = $1
      LIMIT 1
      ${forUpdate ? "FOR UPDATE" : ""}
    `,
    [email],
  );

  return rows[0] ? normalizeUser(rows[0]) : null;
};

const create = async (payload, db = pool) => {
  const result = await db.query(
    `
      INSERT INTO users (
        name, email, password_hash, role, is_active, is_email_verified,
        email_verified_at, wallet_fund, total_fund_added, total_fund_withdrawn,
        wallet_ledger, email_otp_code_hash, email_otp_purpose, email_otp_expires_at, email_otp_last_sent_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10,
        $11::jsonb, $12, $13, $14, $15
      )
      RETURNING *
    `,
    [
      payload.name,
      payload.email,
      payload.password_hash,
      payload.role || "USER",
      payload.is_active ?? true,
      payload.is_email_verified ?? false,
      payload.email_verified_at ?? null,
      toNumber(payload.wallet_fund),
      toNumber(payload.total_fund_added),
      toNumber(payload.total_fund_withdrawn),
      JSON.stringify(ensureArray(payload.wallet_ledger)),
      payload.email_otp_code_hash ?? null,
      payload.email_otp_purpose ?? null,
      payload.email_otp_expires_at ?? null,
      payload.email_otp_last_sent_at ?? null,
    ],
  );

  return normalizeUser(result.rows[0]);
};

const updateWalletState = async (
  userId,
  { wallet_fund, total_fund_added, total_fund_withdrawn, wallet_ledger },
  db = pool,
) => {
  const { rows } = await db.query(
    `
      UPDATE users
      SET
        wallet_fund = $1,
        total_fund_added = $2,
        total_fund_withdrawn = $3,
        wallet_ledger = $4::jsonb,
        updated_at = NOW()
      WHERE id = $5
      RETURNING *
    `,
    [
      toNumber(wallet_fund),
      toNumber(total_fund_added),
      toNumber(total_fund_withdrawn),
      JSON.stringify(ensureArray(wallet_ledger)),
      Number(userId),
    ],
  );

  return rows[0] ? normalizeUser(rows[0]) : null;
};

module.exports = {
  normalizeUser,
  getById,
  getByEmail,
  create,
  updateWalletState,
};
