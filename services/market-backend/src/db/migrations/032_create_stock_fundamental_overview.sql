-- Lean overview table for split fundamentals data.
-- Stores only the overview fields used by the UI and filters.

CREATE TABLE IF NOT EXISTS stock_fundamental_overview (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL UNIQUE REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL UNIQUE REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,

  company_name VARCHAR(255),
  about TEXT,
  key_points TEXT,

  market_cap NUMERIC,
  current_price NUMERIC,
  high_low TEXT,
  stock_pe NUMERIC,
  book_value NUMERIC,
  dividend_yield NUMERIC,
  roce NUMERIC,
  roe NUMERIC,
  face_value NUMERIC,

  pros JSONB NOT NULL DEFAULT '[]'::jsonb,
  cons JSONB NOT NULL DEFAULT '[]'::jsonb,
  links JSONB NOT NULL DEFAULT '[]'::jsonb,

  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_fundamental_overview_market_cap
  ON stock_fundamental_overview (market_cap DESC);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_overview_current_price
  ON stock_fundamental_overview (current_price DESC);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_overview_roe
  ON stock_fundamental_overview (roe DESC);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_overview_roce
  ON stock_fundamental_overview (roce DESC);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_overview_snapshot_id
  ON stock_fundamental_overview (snapshot_id);
