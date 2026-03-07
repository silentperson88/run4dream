-- Refactor EOD storage to one-row-per-stock-per-day.
-- Keeps legacy monthly JSON data in eod_monthly_legacy and migrates data forward.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'eod'
      AND column_name = 'candles'
  ) THEN
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = 'eod_monthly_legacy'
    ) THEN
      ALTER TABLE eod RENAME TO eod_monthly_legacy;
    END IF;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS eod (
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  symbol VARCHAR(64) NOT NULL,
  exchange VARCHAR(8) NOT NULL,
  trade_date DATE NOT NULL,
  open NUMERIC(18, 6) NOT NULL,
  high NUMERIC(18, 6) NOT NULL,
  low NUMERIC(18, 6) NOT NULL,
  close NUMERIC(18, 6) NOT NULL,
  volume BIGINT NOT NULL DEFAULT 0,
  source VARCHAR(32) NOT NULL DEFAULT 'smartapi',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (master_id, trade_date),
  CHECK (exchange IN ('NSE', 'BSE'))
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS eod_default PARTITION OF eod DEFAULT;

DO $$
DECLARE
  y INTEGER;
BEGIN
  FOR y IN 2020..2035 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS eod_%s PARTITION OF eod FOR VALUES FROM (%L) TO (%L)',
      y,
      make_date(y, 1, 1),
      make_date(y + 1, 1, 1)
    );
  END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_eod_trade_date_brin ON eod USING BRIN (trade_date);
CREATE INDEX IF NOT EXISTS idx_eod_master_trade_date_desc ON eod (master_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_eod_symbol_trade_date_desc ON eod (symbol, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_eod_exchange_trade_date_desc ON eod (exchange, trade_date DESC);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'eod_monthly_legacy'
  ) THEN
    INSERT INTO eod (
      master_id,
      symbol,
      exchange,
      trade_date,
      open,
      high,
      low,
      close,
      volume,
      source,
      created_at,
      updated_at
    )
    SELECT
      m.master_id,
      REGEXP_REPLACE(UPPER(SPLIT_PART(m.symbol, '#', 1)), '-EQ$', ''),
      m.exchange,
      (elem->>'date')::date AS trade_date,
      COALESCE((elem->>'o')::numeric, 0)::numeric(18, 6) AS open,
      COALESCE((elem->>'h')::numeric, 0)::numeric(18, 6) AS high,
      COALESCE((elem->>'l')::numeric, 0)::numeric(18, 6) AS low,
      COALESCE((elem->>'c')::numeric, 0)::numeric(18, 6) AS close,
      COALESCE((elem->>'v')::numeric, 0)::bigint AS volume,
      'migration',
      COALESCE(m.created_at, NOW()),
      NOW()
    FROM eod_monthly_legacy m
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(m.candles, '[]'::jsonb)) elem
    ON CONFLICT (master_id, trade_date)
    DO UPDATE SET
      symbol = EXCLUDED.symbol,
      exchange = EXCLUDED.exchange,
      open = COALESCE(eod.open, EXCLUDED.open),
      high = GREATEST(eod.high, EXCLUDED.high),
      low = LEAST(eod.low, EXCLUDED.low),
      close = EXCLUDED.close,
      volume = GREATEST(eod.volume, EXCLUDED.volume),
      source = EXCLUDED.source,
      updated_at = NOW();
  END IF;
END $$;
