-- Quarterly results table for query-first storage.
-- One row per stock per quarter/period.

CREATE TABLE IF NOT EXISTS stock_fundamental_quarterly_results (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,

  period TEXT NOT NULL,
  period_numeric TEXT NOT NULL,

  sales NUMERIC,
  revenue NUMERIC,
  financing_profit NUMERIC,
  financing_margin_percent NUMERIC,
  expenses NUMERIC,
  interest NUMERIC,
  net_profit NUMERIC,
  opm_percent NUMERIC,
  tax_percent NUMERIC,
  depreciation NUMERIC,
  other_income NUMERIC,
  operating_profit NUMERIC,
  profit_before_tax NUMERIC,
  eps NUMERIC,
  raw_pdf TEXT,
  gross_npa_percent NUMERIC,
  net_npa_percent NUMERIC,

  sales_yoy_growth_percent NUMERIC,
  expenses_material_cost_percent NUMERIC,
  expenses_employee_cost_percent NUMERIC,
  other_income_normal NUMERIC,
  net_profit_profit_from_associates NUMERIC,
  net_profit_minority_share NUMERIC,
  net_profit_profit_excl_excep NUMERIC,
  net_profit_profit_for_pe NUMERIC,
  net_profit_profit_for_eps NUMERIC,
  net_profit_exceptional_items NUMERIC,
  net_profit_exceptional_items_at NUMERIC,
  net_profit_yoy_profit_growth_percent NUMERIC,

  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (master_id, period_numeric)
);

CREATE INDEX IF NOT EXISTS idx_stock_fundamental_quarterly_results_master_period
  ON stock_fundamental_quarterly_results (master_id, period_numeric DESC);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_quarterly_results_active_stock_id
  ON stock_fundamental_quarterly_results (active_stock_id);
CREATE INDEX IF NOT EXISTS idx_stock_fundamental_quarterly_results_snapshot_id
  ON stock_fundamental_quarterly_results (snapshot_id);
