ALTER TABLE stock_screener_fundamentals
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_stock_screener_fundamentals_is_active
  ON stock_screener_fundamentals(is_active);
