CREATE TABLE IF NOT EXISTS stock_screener_fundamentals (
  id BIGSERIAL PRIMARY KEY,
  master_id BIGINT NOT NULL UNIQUE REFERENCES stock_master(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL UNIQUE REFERENCES active_stock(id) ON DELETE CASCADE,
  company VARCHAR(255),
  company_info JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  peers JSONB NOT NULL DEFAULT '{}'::jsonb,
  tables JSONB NOT NULL DEFAULT '{}'::jsonb,
  other_details JSONB NOT NULL DEFAULT '{}'::jsonb,
  documents JSONB NOT NULL DEFAULT '{}'::jsonb,
  financials JSONB NOT NULL DEFAULT '{}'::jsonb,
  statements JSONB NOT NULL DEFAULT '{}'::jsonb,
  ratios JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fundamentals_master_id ON stock_screener_fundamentals(master_id);
CREATE INDEX IF NOT EXISTS idx_fundamentals_last_updated ON stock_screener_fundamentals(last_updated_at);
CREATE INDEX IF NOT EXISTS idx_fundamentals_tables_gin ON stock_screener_fundamentals USING GIN (tables);
