CREATE TABLE IF NOT EXISTS stock_master (
  id BIGSERIAL PRIMARY KEY,
  symbol VARCHAR(64) NOT NULL,
  exchange VARCHAR(8) NOT NULL,
  name VARCHAR(255) NOT NULL,
  screener_url TEXT NOT NULL DEFAULT '',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  token VARCHAR(64) UNIQUE,
  raw_stock_id BIGINT REFERENCES rawstocks(id) ON DELETE SET NULL,
  history_range TEXT,
  screener_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  angelone_fetch_status VARCHAR(24) NOT NULL DEFAULT 'not_attempted',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (exchange IN ('NSE', 'BSE'))
);

CREATE INDEX IF NOT EXISTS idx_stock_master_name ON stock_master(name);
CREATE INDEX IF NOT EXISTS idx_stock_master_is_active ON stock_master(is_active);
