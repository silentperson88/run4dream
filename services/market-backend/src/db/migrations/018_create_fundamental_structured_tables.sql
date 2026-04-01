-- Structured fundamentals layer.
-- Keep stock_screener_fundamentals as the raw snapshot backup table.
-- New tables are period-wise / query-friendly and can be backfilled from snapshots.

CREATE TABLE IF NOT EXISTS stock_fundamental_overview (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL UNIQUE REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL UNIQUE REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  company_name VARCHAR(255),
  about TEXT,
  key_points TEXT,
  market_cap NUMERIC,
  current_price NUMERIC,
  high_low TEXT,
  stock_pe NUMERIC,
  book_value NUMERIC,
  dividend_yield NUMERIC,
  roce NUMERIC,
  roe NUMERIC,
  face_value NUMERIC,
  csg_10y TEXT,
  csg_5y TEXT,
  csg_3y TEXT,
  csg_ttm TEXT,
  cpg_10y TEXT,
  cpg_5y TEXT,
  cpg_3y TEXT,
  cpg_ttm TEXT,
  spc_10y TEXT,
  spc_5y TEXT,
  spc_3y TEXT,
  spc_1y TEXT,
  roe_10y TEXT,
  roe_5y TEXT,
  roe_3y TEXT,
  roe_last_year TEXT,
  pros JSONB NOT NULL DEFAULT '[]'::jsonb,
  cons JSONB NOT NULL DEFAULT '[]'::jsonb,
  links JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fundamental_overview_market_cap
  ON stock_fundamental_overview (market_cap DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_overview_current_price
  ON stock_fundamental_overview (current_price DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_overview_roe
  ON stock_fundamental_overview (roe DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_overview_roce
  ON stock_fundamental_overview (roce DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_overview_snapshot_id
  ON stock_fundamental_overview (snapshot_id);

CREATE TABLE IF NOT EXISTS stock_fundamental_peers_snapshot (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL UNIQUE REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL UNIQUE REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  rows JSONB NOT NULL DEFAULT '[]'::jsonb,
  table_class TEXT,
  source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fundamental_peers_snapshot_last_updated
  ON stock_fundamental_peers_snapshot (last_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_peers_snapshot_snapshot_id
  ON stock_fundamental_peers_snapshot (snapshot_id);

CREATE TABLE IF NOT EXISTS stock_fundamental_quarterly_results (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  sales NUMERIC,
  revenue NUMERIC,
  financing_profit NUMERIC,
  financing_margin_percent NUMERIC,
  raw_pdf TEXT,
  expenses NUMERIC,
  operating_profit NUMERIC,
  opm_percent NUMERIC,
  other_income NUMERIC,
  other_income_normal TEXT,
  interest NUMERIC,
  depreciation NUMERIC,
  profit_before_tax NUMERIC,
  tax_percent NUMERIC,
  net_profit NUMERIC,
  eps NUMERIC,
  sales_growth_percent TEXT,
  yoy_sales_growth_percent TEXT,
  yoy_profit_growth_percent TEXT,
  exceptional_items TEXT,
  profit_excl_excep TEXT,
  exceptional_items_at TEXT,
  profit_for_eps TEXT,
  minority_share TEXT,
  profit_for_pe TEXT,
  material_cost_percent TEXT,
  employee_cost_percent TEXT,
  manufacturing_cost_percent TEXT,
  other_cost_percent TEXT,
  gross_npa_percent NUMERIC,
  net_npa_percent NUMERIC,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_results_master_period
  ON stock_fundamental_quarterly_results (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_results_snapshot_id
  ON stock_fundamental_quarterly_results (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_results_net_profit
  ON stock_fundamental_quarterly_results (net_profit DESC);

CREATE TABLE IF NOT EXISTS stock_fundamental_profit_loss_periods (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  sales NUMERIC,
  revenue NUMERIC,
  financing_profit NUMERIC,
  financing_margin_percent NUMERIC,
  expenses NUMERIC,
  operating_profit NUMERIC,
  opm_percent NUMERIC,
  other_income NUMERIC,
  interest NUMERIC,
  depreciation NUMERIC,
  profit_before_tax NUMERIC,
  tax_percent NUMERIC,
  net_profit NUMERIC,
  eps NUMERIC,
  dividend_payout_percent NUMERIC,
  sales_growth_percent TEXT,
  yoy_sales_growth_percent TEXT,
  yoy_profit_growth_percent TEXT,
  exceptional_items TEXT,
  profit_excl_excep TEXT,
  exceptional_items_at TEXT,
  profit_for_eps TEXT,
  minority_share TEXT,
  profit_for_pe TEXT,
  material_cost_percent TEXT,
  employee_cost_percent TEXT,
  manufacturing_cost_percent TEXT,
  other_cost_percent TEXT,
  csg_10y TEXT,
  csg_5y TEXT,
  csg_3y TEXT,
  csg_ttm TEXT,
  cpg_10y TEXT,
  cpg_5y TEXT,
  cpg_3y TEXT,
  cpg_ttm TEXT,
  spc_10y TEXT,
  spc_5y TEXT,
  spc_3y TEXT,
  spc_1y TEXT,
  roe_10y TEXT,
  roe_5y TEXT,
  roe_3y TEXT,
  roe_last_year TEXT,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_profit_loss_master_period
  ON stock_fundamental_profit_loss_periods (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_profit_loss_snapshot_id
  ON stock_fundamental_profit_loss_periods (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_fundamental_profit_loss_net_profit
  ON stock_fundamental_profit_loss_periods (net_profit DESC);

CREATE TABLE IF NOT EXISTS stock_fundamental_balance_sheet_periods (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  equity_capital NUMERIC,
  reserves NUMERIC,
  borrowing NUMERIC,
  deposits NUMERIC,
  borrowings NUMERIC,
  long_term_borrowings NUMERIC,
  short_term_borrowings NUMERIC,
  other_borrowings NUMERIC,
  other_liabilities NUMERIC,
  advance_from_customers NUMERIC,
  lease_liabilities NUMERIC,
  trade_payables NUMERIC,
  other_liability_items NUMERIC,
  non_controlling_int NUMERIC,
  total_liabilities NUMERIC,
  fixed_assets NUMERIC,
  gross_block NUMERIC,
  accumulated_depreciation NUMERIC,
  building NUMERIC,
  land NUMERIC,
  plant_machinery NUMERIC,
  railway_sidings NUMERIC,
  vehicles NUMERIC,
  computers NUMERIC,
  furniture_n_fittings NUMERIC,
  equipments NUMERIC,
  other_fixed_assets NUMERIC,
  intangible_assets NUMERIC,
  cwip NUMERIC,
  investments NUMERIC,
  other_assets NUMERIC,
  inventories NUMERIC,
  trade_receivables NUMERIC,
  cash_equivalents NUMERIC,
  loans_n_advances NUMERIC,
  other_asset_items NUMERIC,
  total_assets NUMERIC,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_balance_sheet_master_period
  ON stock_fundamental_balance_sheet_periods (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_balance_sheet_snapshot_id
  ON stock_fundamental_balance_sheet_periods (snapshot_id);

CREATE TABLE IF NOT EXISTS stock_fundamental_cash_flow_periods (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  cash_from_operating_activity NUMERIC,
  profit_from_operations NUMERIC,
  working_capital_changes NUMERIC,
  receivables NUMERIC,
  inventory NUMERIC,
  payables NUMERIC,
  other_wc_items NUMERIC,
  direct_taxes NUMERIC,
  interest_received NUMERIC,
  dividends_received NUMERIC,
  exceptional_cf_items NUMERIC,
  cash_from_investing_activity NUMERIC,
  investments_purchased NUMERIC,
  investments_sold NUMERIC,
  fixed_assets_purchased NUMERIC,
  fixed_assets_sold NUMERIC,
  acquisition_of_companies NUMERIC,
  invest_in_subsidiaries NUMERIC,
  loans_advances NUMERIC,
  other_investing_items NUMERIC,
  cash_from_financing_activity NUMERIC,
  proceeds_from_borrowings NUMERIC,
  repayment_of_borrowings NUMERIC,
  interest_paid_fin NUMERIC,
  dividends_paid NUMERIC,
  financial_liabilities NUMERIC,
  share_application_money NUMERIC,
  other_financing_items NUMERIC,
  net_cash_flow NUMERIC,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_cash_flow_master_period
  ON stock_fundamental_cash_flow_periods (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_cash_flow_snapshot_id
  ON stock_fundamental_cash_flow_periods (snapshot_id);

CREATE TABLE IF NOT EXISTS stock_fundamental_ratios_periods (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  debtor_days NUMERIC,
  inventory_days NUMERIC,
  days_payable NUMERIC,
  cash_conversion_cycle NUMERIC,
  working_capital_days NUMERIC,
  roce_percent NUMERIC,
  roe_percent NUMERIC,
  gross_npa_percent NUMERIC,
  net_npa_percent NUMERIC,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_ratios_master_period
  ON stock_fundamental_ratios_periods (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_ratios_snapshot_id
  ON stock_fundamental_ratios_periods (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_fundamental_ratios_roce
  ON stock_fundamental_ratios_periods (roce_percent DESC);

CREATE TABLE IF NOT EXISTS stock_fundamental_shareholdings_periods (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL REFERENCES active_stock(id) ON DELETE CASCADE,
  snapshot_id BIGINT REFERENCES stock_screener_fundamentals(id) ON DELETE SET NULL,
  period_label TEXT NOT NULL,
  period_end DATE,
  period_index INTEGER,
  title TEXT,
  headers JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
  row_label TEXT,
  promoters NUMERIC,
  fiis NUMERIC,
  diis NUMERIC,
  public NUMERIC,
  others NUMERIC,
  no_of_shareholders NUMERIC,
  children JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_id, period_label)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_shareholdings_master_period
  ON stock_fundamental_shareholdings_periods (master_id, period_end DESC NULLS LAST, period_index DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fundamental_shareholdings_snapshot_id
  ON stock_fundamental_shareholdings_periods (snapshot_id);

