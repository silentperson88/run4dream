CREATE TABLE IF NOT EXISTS stock_master (
  id BIGSERIAL PRIMARY KEY,
  company VARCHAR(255),
  symbol VARCHAR(64) NOT NULL,
  exchange VARCHAR(8) NOT NULL,
  name VARCHAR(255) NOT NULL,
  sector VARCHAR(255),
  industry VARCHAR(255),
  screener_url TEXT NOT NULL DEFAULT '',
  fetch_count INTEGER NOT NULL DEFAULT 0,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  token VARCHAR(64) NOT NULL UNIQUE,
  raw_stock_id BIGINT REFERENCES rawstocks(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (exchange IN ('NSE', 'BSE'))
);

CREATE INDEX IF NOT EXISTS idx_stock_master_name ON stock_master(name);
CREATE INDEX IF NOT EXISTS idx_stock_master_is_active ON stock_master(is_active);
