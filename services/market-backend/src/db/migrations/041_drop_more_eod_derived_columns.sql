ALTER TABLE eod
  DROP COLUMN IF EXISTS price_vs_dma_50_pct,
  DROP COLUMN IF EXISTS price_vs_dma_200_pct,
  DROP COLUMN IF EXISTS dma_50_vs_dma_200,
  DROP COLUMN IF EXISTS return_1w,
  DROP COLUMN IF EXISTS distance_from_52w_high_pct,
  DROP COLUMN IF EXISTS distance_from_ath_pct,
  DROP COLUMN IF EXISTS volume_ratio,
  DROP COLUMN IF EXISTS atr_pct,
  DROP COLUMN IF EXISTS macd_histogram,
  DROP COLUMN IF EXISTS supertrend;
