CREATE TABLE IF NOT EXISTS rawstocks (
  id BIGSERIAL PRIMARY KEY,
  token VARCHAR(64) UNIQUE,
  symbol VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL UNIQUE,
  exch_seg VARCHAR(32) NOT NULL,
  instrumenttype VARCHAR(32) NOT NULL DEFAULT 'EQ',
  lotsize INTEGER NOT NULL DEFAULT 1,
  tick_size NUMERIC(18,8),
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('pending', 'missing_token', 'approved', 'rejected'))
);
