ALTER TABLE IF EXISTS stock_fundamental_quarterly_results
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_quarterly_results
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_quarterly_results
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;

ALTER TABLE IF EXISTS stock_fundamental_profit_loss_periods
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_profit_loss_periods
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_profit_loss_periods
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;

ALTER TABLE IF EXISTS stock_fundamental_balance_sheet_periods
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_balance_sheet_periods
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_balance_sheet_periods
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;

ALTER TABLE IF EXISTS stock_fundamental_cash_flow_periods
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_cash_flow_periods
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_cash_flow_periods
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;

ALTER TABLE IF EXISTS stock_fundamental_ratios_periods
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_ratios_periods
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_ratios_periods
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;

ALTER TABLE IF EXISTS stock_fundamental_shareholding_periods
  ADD COLUMN IF NOT EXISTS period_label TEXT,
  ADD COLUMN IF NOT EXISTS period_end DATE,
  ADD COLUMN IF NOT EXISTS period_index INTEGER,
  ADD COLUMN IF NOT EXISTS title TEXT,
  ADD COLUMN IF NOT EXISTS headers JSONB,
  ADD COLUMN IF NOT EXISTS raw_row JSONB,
  ADD COLUMN IF NOT EXISTS row_label TEXT;

UPDATE stock_fundamental_shareholding_periods
SET period_label = COALESCE(period_label, period)
WHERE period_label IS NULL;

UPDATE stock_fundamental_shareholding_periods
SET row_label = COALESCE(row_label, period_label, period)
WHERE row_label IS NULL;
