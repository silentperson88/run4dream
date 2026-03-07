ALTER TABLE stock_screener_fundamentals
  ADD COLUMN IF NOT EXISTS quarters_table JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS profit_loss_table JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS balance_sheet_table JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS cash_flow_table JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS ratios_table JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS shareholdings_table JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Backfill section columns from legacy "tables" payload where present.
UPDATE stock_screener_fundamentals
SET
  quarters_table = CASE
    WHEN quarters_table = '{}'::jsonb AND jsonb_typeof(tables -> 'quarters') = 'object' THEN tables -> 'quarters'
    ELSE quarters_table
  END,
  profit_loss_table = CASE
    WHEN profit_loss_table = '{}'::jsonb AND jsonb_typeof(tables -> 'profit_loss') = 'object' THEN tables -> 'profit_loss'
    ELSE profit_loss_table
  END,
  balance_sheet_table = CASE
    WHEN balance_sheet_table = '{}'::jsonb AND jsonb_typeof(tables -> 'balance_sheet') = 'object' THEN tables -> 'balance_sheet'
    ELSE balance_sheet_table
  END,
  cash_flow_table = CASE
    WHEN cash_flow_table = '{}'::jsonb AND jsonb_typeof(tables -> 'cash_flow') = 'object' THEN tables -> 'cash_flow'
    ELSE cash_flow_table
  END,
  ratios_table = CASE
    WHEN ratios_table = '{}'::jsonb AND jsonb_typeof(tables -> 'ratios') = 'object' THEN tables -> 'ratios'
    ELSE ratios_table
  END,
  shareholdings_table = CASE
    WHEN shareholdings_table = '{}'::jsonb AND jsonb_typeof(tables -> 'shareholdings') = 'object' THEN tables -> 'shareholdings'
    ELSE shareholdings_table
  END;

CREATE INDEX IF NOT EXISTS idx_fundamentals_quarters_table_gin
  ON stock_screener_fundamentals USING GIN (quarters_table);
CREATE INDEX IF NOT EXISTS idx_fundamentals_profit_loss_table_gin
  ON stock_screener_fundamentals USING GIN (profit_loss_table);
CREATE INDEX IF NOT EXISTS idx_fundamentals_balance_sheet_table_gin
  ON stock_screener_fundamentals USING GIN (balance_sheet_table);
CREATE INDEX IF NOT EXISTS idx_fundamentals_cash_flow_table_gin
  ON stock_screener_fundamentals USING GIN (cash_flow_table);
CREATE INDEX IF NOT EXISTS idx_fundamentals_ratios_table_gin
  ON stock_screener_fundamentals USING GIN (ratios_table);
CREATE INDEX IF NOT EXISTS idx_fundamentals_shareholdings_table_gin
  ON stock_screener_fundamentals USING GIN (shareholdings_table);
