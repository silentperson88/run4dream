CREATE TABLE IF NOT EXISTS eod (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  symbol VARCHAR(64) NOT NULL,
  exchange VARCHAR(8) NOT NULL,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  candles JSONB NOT NULL DEFAULT '[]'::jsonb,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(master_id, year, month),
  CHECK (exchange IN ('NSE', 'BSE')),
  CHECK (month >= 1 AND month <= 12)
);

CREATE INDEX IF NOT EXISTS idx_eod_master_year_month ON eod(master_id, year, month);
