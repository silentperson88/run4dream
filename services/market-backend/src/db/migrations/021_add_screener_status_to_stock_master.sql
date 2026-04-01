ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS screener_status VARCHAR(16) NOT NULL DEFAULT 'PENDING';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_stock_master_screener_status'
  ) THEN
    ALTER TABLE stock_master
      ADD CONSTRAINT chk_stock_master_screener_status
      CHECK (screener_status IN ('PENDING', 'VALID', 'FAILED'));
  END IF;
END $$;

UPDATE stock_master
SET screener_status = CASE
  WHEN screener_url IS NULL OR screener_url = '' THEN 'FAILED'
  ELSE COALESCE(screener_status, 'PENDING')
END;

CREATE INDEX IF NOT EXISTS idx_stock_master_screener_status
  ON stock_master(screener_status);
