ALTER TABLE stock_master
ADD COLUMN IF NOT EXISTS eod_history_status VARCHAR(32) NOT NULL DEFAULT 'NO_EOD_DATA';

UPDATE stock_master sm
SET eod_history_status = CASE
  WHEN EXISTS (
    SELECT 1
    FROM eod e
    WHERE e.master_id = sm.id
    LIMIT 1
  ) THEN 'HAS_EOD_DATA'
  WHEN NULLIF(BTRIM(sm.history_range), '') IS NOT NULL THEN 'HAS_EOD_DATA'
  ELSE 'NO_EOD_DATA'
END;

CREATE INDEX IF NOT EXISTS idx_stock_master_eod_history_status
ON stock_master (eod_history_status);
