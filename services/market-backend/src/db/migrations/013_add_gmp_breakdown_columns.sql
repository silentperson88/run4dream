ALTER TABLE live_ipo_gmp
ADD COLUMN IF NOT EXISTS gmp_current TEXT,
ADD COLUMN IF NOT EXISTS gmp_gain_percentage TEXT,
ADD COLUMN IF NOT EXISTS gmp_min_value TEXT,
ADD COLUMN IF NOT EXISTS gmp_max_value TEXT;

CREATE INDEX IF NOT EXISTS idx_live_ipo_gmp_gmp_current ON live_ipo_gmp (gmp_current);
