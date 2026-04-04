ALTER TABLE IF EXISTS active_stock
  DROP COLUMN IF EXISTS is_active;

DROP INDEX IF EXISTS idx_active_stock_is_active;

ALTER TABLE IF EXISTS stock_screener_fundamentals
  DROP COLUMN IF EXISTS is_active;

DROP INDEX IF EXISTS idx_stock_screener_fundamentals_is_active;
