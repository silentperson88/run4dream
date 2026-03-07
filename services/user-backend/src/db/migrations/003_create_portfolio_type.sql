CREATE TABLE IF NOT EXISTS portfolio_type (
  id BIGSERIAL PRIMARY KEY,
  code VARCHAR(64) NOT NULL UNIQUE,
  display_name VARCHAR(255) NOT NULL,
  description TEXT NOT NULL,
  fund NUMERIC(18,2),
  risk_level VARCHAR(16) NOT NULL DEFAULT 'NONE',
  rules_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  important_notes JSONB NOT NULL DEFAULT '[]'::jsonb,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (risk_level IN ('NONE','LOW','MEDIUM','HIGH'))
);

CREATE INDEX IF NOT EXISTS idx_portfolio_type_is_active ON portfolio_type(is_active);
CREATE INDEX IF NOT EXISTS idx_portfolio_type_code ON portfolio_type(code);
