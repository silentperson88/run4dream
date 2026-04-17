ALTER TABLE stock_master
ALTER COLUMN eod_history_status DROP NOT NULL;

ALTER TABLE stock_master
ALTER COLUMN eod_history_status DROP DEFAULT;

UPDATE stock_master sm
SET eod_history_status = NULL
WHERE sm.eod_history_status = 'NO_EOD_DATA'
  AND NOT EXISTS (
    SELECT 1
    FROM eod e
    WHERE e.master_id = sm.id
    LIMIT 1
  )
  AND NULLIF(BTRIM(sm.history_range), '') IS NULL;
