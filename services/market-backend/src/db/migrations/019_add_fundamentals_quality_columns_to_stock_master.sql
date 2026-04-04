ALTER TABLE stock_master
  ADD COLUMN IF NOT EXISTS fundamentals_checked_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS fundamentals_failed_fields TEXT[],
  ADD COLUMN IF NOT EXISTS fundamentals_failed_reason TEXT;
