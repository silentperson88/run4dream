ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS simulation_mode VARCHAR(16) NOT NULL DEFAULT 'LIVE';

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS simulated_trade_date DATE;

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_status_check;

ALTER TABLE orders
  ADD CONSTRAINT orders_status_check
  CHECK (status IN ('OPEN','PARTIALLY_FILLED','COMPLETED','CANCELLED'));

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_simulation_mode_check;

ALTER TABLE orders
  ADD CONSTRAINT orders_simulation_mode_check
  CHECK (simulation_mode IN ('LIVE','BACKTEST'));

