CREATE TABLE IF NOT EXISTS tokens (
  id BIGSERIAL PRIMARY KEY,
  totp VARCHAR(128) NOT NULL,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  feed_token TEXT,
  expiry_time TIMESTAMPTZ NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  generated_by VARCHAR(64) NOT NULL DEFAULT 'api',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  market_open_time VARCHAR(8) NOT NULL DEFAULT '09:15',
  market_close_time VARCHAR(8) NOT NULL DEFAULT '15:30',
  scheduler_state VARCHAR(32) NOT NULL DEFAULT 'NOT_STARTED',
  scheduler_current_phase VARCHAR(32) NOT NULL DEFAULT 'NONE',
  last_phase_completed_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (scheduler_state IN ('NOT_STARTED', 'RUNNING', 'PAUSED', 'COMPLETED')),
  CHECK (scheduler_current_phase IN ('FULL', 'LTP', 'OHLC', 'NONE'))
);

CREATE INDEX IF NOT EXISTS idx_tokens_expiry_time ON tokens(expiry_time);
CREATE INDEX IF NOT EXISTS idx_tokens_is_active ON tokens(is_active);
