DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'live_ipo_gmp' AND column_name = 'open'
  ) THEN
    ALTER TABLE live_ipo_gmp RENAME COLUMN open TO open_date;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'live_ipo_gmp' AND column_name = 'close'
  ) THEN
    ALTER TABLE live_ipo_gmp RENAME COLUMN close TO close_date;
  END IF;
END $$;

ALTER TABLE live_ipo_gmp
ADD COLUMN IF NOT EXISTS open_gmp TEXT,
ADD COLUMN IF NOT EXISTS close_gmp TEXT,
ADD COLUMN IF NOT EXISTS listing_gmp TEXT;
