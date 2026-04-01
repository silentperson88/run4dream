ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS fundamentals_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  ADD COLUMN IF NOT EXISTS fundamentals_checked_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS fundamentals_failed_fields TEXT[],
  ADD COLUMN IF NOT EXISTS fundamentals_failed_reason TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_stock_master_fundamentals_status'
  ) THEN
    ALTER TABLE stock_master
      ADD CONSTRAINT chk_stock_master_fundamentals_status
      CHECK (fundamentals_status IN ('PENDING', 'VALID', 'PARTIAL', 'FAILED'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_stock_master_fundamentals_status
  ON stock_master(fundamentals_status);
