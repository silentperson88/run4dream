ALTER TABLE stock_master
ADD COLUMN IF NOT EXISTS has_history_data BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS history_data_from_date DATE,
ADD COLUMN IF NOT EXISTS history_data_to_date DATE,
ADD COLUMN IF NOT EXISTS history_requested_from_date DATE,
ADD COLUMN IF NOT EXISTS history_requested_to_date DATE;

CREATE INDEX IF NOT EXISTS idx_stock_master_has_history_data
  ON stock_master(has_history_data);
