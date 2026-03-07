CREATE TABLE IF NOT EXISTS user_portfolios (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  portfolio_type_id BIGINT NOT NULL REFERENCES portfolio_type(id) ON DELETE RESTRICT,
  name VARCHAR(255) NOT NULL,
  initial_fund NUMERIC(18,2) NOT NULL DEFAULT 0,
  available_fund NUMERIC(18,2) NOT NULL DEFAULT 0,
  lock_fund JSONB NOT NULL DEFAULT '[]'::jsonb,
  holdings JSONB NOT NULL DEFAULT '[]'::jsonb,
  status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('ACTIVE','INACTIVE','ARCHIVED'))
);

CREATE INDEX IF NOT EXISTS idx_user_portfolios_user_id ON user_portfolios(user_id);
CREATE INDEX IF NOT EXISTS idx_user_portfolios_status ON user_portfolios(status);
CREATE INDEX IF NOT EXISTS idx_user_portfolios_user_type ON user_portfolios(user_id, portfolio_type_id);
