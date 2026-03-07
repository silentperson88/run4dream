CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(320) NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role VARCHAR(32) NOT NULL DEFAULT 'USER',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  is_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
  email_verified_at TIMESTAMPTZ,
  email_otp_code_hash TEXT,
  email_otp_purpose VARCHAR(32),
  email_otp_expires_at TIMESTAMPTZ,
  email_otp_last_sent_at TIMESTAMPTZ,
  wallet_fund NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (wallet_fund >= 0),
  total_fund_added NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (total_fund_added >= 0),
  total_fund_withdrawn NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (total_fund_withdrawn >= 0),
  wallet_ledger JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (role IN ('SUPERADMIN', 'USER')),
  CHECK (email_otp_purpose IS NULL OR email_otp_purpose IN ('VERIFY_EMAIL', 'RESET_PASSWORD'))
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_is_email_verified ON users(is_email_verified);
CREATE INDEX IF NOT EXISTS idx_users_email_otp_expires_at ON users(email_otp_expires_at);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC);
