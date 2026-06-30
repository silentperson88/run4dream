ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS historical_universe_state TEXT;

CREATE INDEX IF NOT EXISTS idx_stock_master_historical_universe_state
  ON stock_master(historical_universe_state);
