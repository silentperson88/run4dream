DO $$
DECLARE
  has_history_from_date boolean := FALSE;
  has_history_to_date boolean := FALSE;
BEGIN
  ALTER TABLE stock_master
    ADD COLUMN IF NOT EXISTS history_range TEXT;

  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'stock_master'
      AND column_name = 'history_data_from_date'
  ) INTO has_history_from_date;

  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'stock_master'
      AND column_name = 'history_data_to_date'
  ) INTO has_history_to_date;

  IF has_history_from_date OR has_history_to_date THEN
    EXECUTE $sql$
      UPDATE stock_master
      SET history_range = CASE
        WHEN history_range IS NOT NULL AND btrim(history_range) <> '' THEN history_range
        WHEN history_data_from_date IS NULL AND history_data_to_date IS NULL THEN history_range
        WHEN history_data_from_date IS NULL THEN to_char(history_data_to_date, 'YYYY-MM-DD')
        WHEN history_data_to_date IS NULL THEN to_char(history_data_from_date, 'YYYY-MM-DD')
        ELSE to_char(history_data_from_date, 'YYYY-MM-DD') || ' to ' || to_char(history_data_to_date, 'YYYY-MM-DD')
      END
    $sql$;
  END IF;
END $$;

ALTER TABLE stock_master
  DROP COLUMN IF EXISTS company,
  DROP COLUMN IF EXISTS sector,
  DROP COLUMN IF EXISTS industry,
  DROP COLUMN IF EXISTS fetch_count,
  DROP COLUMN IF EXISTS has_history_data,
  DROP COLUMN IF EXISTS history_data_from_date,
  DROP COLUMN IF EXISTS history_data_to_date,
  DROP COLUMN IF EXISTS history_requested_from_date,
  DROP COLUMN IF EXISTS history_requested_to_date,
  DROP COLUMN IF EXISTS fundamentals_status,
  DROP COLUMN IF EXISTS fundamentals_checked_at,
  DROP COLUMN IF EXISTS fundamentals_failed_fields,
  DROP COLUMN IF EXISTS fundamentals_failed_reason;
