DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_stock_master_screener_status'
  ) THEN
    ALTER TABLE stock_master DROP CONSTRAINT chk_stock_master_screener_status;
  END IF;

  ALTER TABLE stock_master
    ADD CONSTRAINT chk_stock_master_screener_status
    CHECK (screener_status IN ('PENDING', 'VALID', 'FAILED', 'FAILED_NO_RETRY'));
END $$;

UPDATE stock_master
SET screener_status = CASE
  WHEN screener_status IS NULL OR screener_status = '' THEN 'PENDING'
  WHEN screener_status NOT IN ('PENDING', 'VALID', 'FAILED', 'FAILED_NO_RETRY') THEN 'PENDING'
  ELSE screener_status
END;
