DROP INDEX IF EXISTS idx_stock_master_historical_universe_state;

ALTER TABLE stock_master
  DROP COLUMN IF EXISTS historical_universe_state;
