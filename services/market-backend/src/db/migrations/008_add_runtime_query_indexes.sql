CREATE INDEX IF NOT EXISTS idx_stock_master_active_created_at
  ON stock_master(is_active, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rawstocks_symbol_status
  ON rawstocks(symbol, status);

CREATE INDEX IF NOT EXISTS idx_tokens_generated_active
  ON tokens(generated_at DESC, is_active);
