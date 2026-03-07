CREATE TABLE IF NOT EXISTS live_ipo_gmp (
  id BIGSERIAL PRIMARY KEY,
  ipo_name TEXT NOT NULL,
  ipo_price TEXT,
  gmp TEXT,
  gmp_rating TEXT,
  subscription TEXT,
  sub_type TEXT,
  est_listing_price TEXT,
  est_listing_gain_pct TEXT,
  open_date TEXT,
  close_date TEXT,
  allotment_date TEXT,
  listing_date TEXT,
  status TEXT,
  source_url TEXT NOT NULL,
  identity_hash TEXT NOT NULL UNIQUE,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_ipo_gmp_name ON live_ipo_gmp (ipo_name);
CREATE INDEX IF NOT EXISTS idx_live_ipo_gmp_listing_date ON live_ipo_gmp (listing_date);
CREATE INDEX IF NOT EXISTS idx_live_ipo_gmp_fetched_at ON live_ipo_gmp (fetched_at);
