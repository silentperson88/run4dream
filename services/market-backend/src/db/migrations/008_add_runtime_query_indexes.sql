CREATE INDEX IF NOT EXISTS idx_active_stock_active_added_at
  ON active_stock(is_active, added_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_master_active_created_at
  ON stock_master(is_active, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_master_company_trgm
  ON stock_master USING GIN (lower(company) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_rawstocks_symbol_status
  ON rawstocks(symbol, status);

CREATE INDEX IF NOT EXISTS idx_tokens_generated_active
  ON tokens(generated_at DESC, is_active);
