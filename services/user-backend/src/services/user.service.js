const bcrypt = require("bcrypt");
const userRepo = require("../repositories/users.repository");

async function createUser(data) {
  const hashed = await bcrypt.hash(data.password, 10);
  return userRepo.create({
    name: data.name,
    email: data.email,
    password_hash: hashed,
    role: data.role || "USER",
    is_active: data.isActive ?? true,
    is_email_verified: data.isEmailVerified ?? false,
    email_verified_at: data.emailVerifiedAt ?? null,
    wallet_fund: Number(data.wallet_fund || 0),
    total_fund_added: Number(data.total_fund_added || 0),
    total_fund_withdrawn: Number(data.total_fund_withdrawn || 0),
    wallet_ledger: Array.isArray(data.wallet_ledger) ? data.wallet_ledger : [],
    email_otp_code_hash: data.email_otp?.code_hash ?? null,
    email_otp_purpose: data.email_otp?.purpose ?? null,
    email_otp_expires_at: data.email_otp?.expires_at ?? null,
    email_otp_last_sent_at: data.email_otp?.last_sent_at ?? null,
  });
}

async function findUserByEmail(email) {
  return userRepo.getByEmail(email);
}

module.exports = { createUser, findUserByEmail };
