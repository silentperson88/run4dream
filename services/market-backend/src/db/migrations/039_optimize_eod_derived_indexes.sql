DROP INDEX IF EXISTS idx_eod_is_liquid_trade_date;

CREATE INDEX IF NOT EXISTS idx_eod_is_liquid_true_trade_date
  ON eod (trade_date DESC)
  WHERE is_liquid = TRUE;
