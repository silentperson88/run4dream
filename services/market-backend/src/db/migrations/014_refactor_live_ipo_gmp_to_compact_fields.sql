ALTER TABLE live_ipo_gmp
ADD COLUMN IF NOT EXISTS name TEXT,
ADD COLUMN IF NOT EXISTS gain_price TEXT,
ADD COLUMN IF NOT EXISTS gain_percentage TEXT,
ADD COLUMN IF NOT EXISTS ai_score NUMERIC,
ADD COLUMN IF NOT EXISTS rating INTEGER,
ADD COLUMN IF NOT EXISTS subscribed TEXT,
ADD COLUMN IF NOT EXISTS price TEXT,
ADD COLUMN IF NOT EXISTS ipo_size TEXT,
ADD COLUMN IF NOT EXISTS lot TEXT,
ADD COLUMN IF NOT EXISTS open TEXT,
ADD COLUMN IF NOT EXISTS close TEXT,
ADD COLUMN IF NOT EXISTS boarding_date TEXT,
ADD COLUMN IF NOT EXISTS institutional_backing SMALLINT,
ADD COLUMN IF NOT EXISTS type TEXT;

UPDATE live_ipo_gmp
SET
  name = COALESCE(name, ipo_name),
  gain_price = COALESCE(gain_price, gmp_current, gmp),
  gain_percentage = COALESCE(gain_percentage, gmp_gain_percentage),
  subscribed = COALESCE(subscribed, subscription),
  price = COALESCE(price, ipo_price),
  open = COALESCE(open, open_date),
  close = COALESCE(close, close_date),
  boarding_date = COALESCE(boarding_date, allotment_date),
  type = COALESCE(type, CASE WHEN COALESCE(name, ipo_name) ~* 'SME\\s*$' THEN 'SME' ELSE 'MAINBOARD' END)
WHERE name IS NULL
   OR gain_price IS NULL
   OR gain_percentage IS NULL
   OR subscribed IS NULL
   OR price IS NULL
   OR open IS NULL
   OR close IS NULL
   OR boarding_date IS NULL
   OR type IS NULL;

ALTER TABLE live_ipo_gmp
ALTER COLUMN name SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_live_ipo_gmp_name ON live_ipo_gmp (name);

ALTER TABLE live_ipo_gmp
DROP COLUMN IF EXISTS ipo_name,
DROP COLUMN IF EXISTS ipo_price,
DROP COLUMN IF EXISTS gmp,
DROP COLUMN IF EXISTS gmp_current,
DROP COLUMN IF EXISTS gmp_gain_percentage,
DROP COLUMN IF EXISTS gmp_min_value,
DROP COLUMN IF EXISTS gmp_max_value,
DROP COLUMN IF EXISTS gmp_rating,
DROP COLUMN IF EXISTS subscription,
DROP COLUMN IF EXISTS sub_type,
DROP COLUMN IF EXISTS est_listing_price,
DROP COLUMN IF EXISTS est_listing_gain_pct,
DROP COLUMN IF EXISTS open_date,
DROP COLUMN IF EXISTS close_date,
DROP COLUMN IF EXISTS allotment_date,
DROP COLUMN IF EXISTS source_url,
DROP COLUMN IF EXISTS identity_hash,
DROP COLUMN IF EXISTS raw_row,
DROP COLUMN IF EXISTS fetched_at,
DROP COLUMN IF EXISTS created_at,
DROP COLUMN IF EXISTS updated_at;
