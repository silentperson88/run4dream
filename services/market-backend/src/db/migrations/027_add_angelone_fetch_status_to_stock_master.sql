ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS angelone_fetch_status VARCHAR(24) NOT NULL DEFAULT 'not_attempted';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_stock_master_angelone_fetch_status'
  ) THEN
    ALTER TABLE stock_master
      ADD CONSTRAINT chk_stock_master_angelone_fetch_status
      CHECK (angelone_fetch_status IN ('not_attempted', 'fetched', 'unfetched', 'skipped_tokenless', 'failed', 'unknown'));
  END IF;
END $$;
