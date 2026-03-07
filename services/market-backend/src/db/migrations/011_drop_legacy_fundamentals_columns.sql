DROP INDEX IF EXISTS idx_fundamentals_tables_gin;

ALTER TABLE stock_screener_fundamentals
  DROP COLUMN IF EXISTS tables,
  DROP COLUMN IF EXISTS raw_payload,
  DROP COLUMN IF EXISTS financials,
  DROP COLUMN IF EXISTS statements,
  DROP COLUMN IF EXISTS ratios;
