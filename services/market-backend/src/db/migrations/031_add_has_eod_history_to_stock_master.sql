ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS has_eod_history BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE stock_master sm
SET has_eod_history = EXISTS (
  SELECT 1
  FROM eod e
  WHERE e.master_id = sm.id
);

CREATE INDEX IF NOT EXISTS idx_stock_master_has_eod_history
  ON stock_master(has_eod_history);
