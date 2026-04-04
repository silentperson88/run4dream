-- Performance-focused indexes for current API query patterns.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- rawstocks: admin listing/search/filter
CREATE INDEX IF NOT EXISTS idx_rawstocks_status ON rawstocks(status);
CREATE INDEX IF NOT EXISTS idx_rawstocks_symbol ON rawstocks(symbol);
CREATE INDEX IF NOT EXISTS idx_rawstocks_name_trgm ON rawstocks USING GIN (lower(name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_rawstocks_symbol_trgm ON rawstocks USING GIN (lower(symbol) gin_trgm_ops);

-- stock_master: search/listing + active/screener/fetch flow
CREATE INDEX IF NOT EXISTS idx_stock_master_symbol ON stock_master(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_master_token ON stock_master(token);
CREATE INDEX IF NOT EXISTS idx_stock_master_screener_url ON stock_master(screener_url);
CREATE INDEX IF NOT EXISTS idx_stock_master_name_trgm ON stock_master USING GIN (lower(name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_stock_master_symbol_trgm ON stock_master USING GIN (lower(symbol) gin_trgm_ops);

-- active_stock: live quote updates/toggles + lookup by token/master
CREATE INDEX IF NOT EXISTS idx_active_stock_master_id ON active_stock(master_id);
CREATE INDEX IF NOT EXISTS idx_active_stock_symbol ON active_stock(symbol);
CREATE INDEX IF NOT EXISTS idx_active_stock_exchange ON active_stock(exchange);
CREATE INDEX IF NOT EXISTS idx_active_stock_last_update ON active_stock(last_update);
CREATE INDEX IF NOT EXISTS idx_active_stock_name_trgm ON active_stock USING GIN (lower(name) gin_trgm_ops);

-- fundamentals: refresh scheduler + per-stock fetch
CREATE INDEX IF NOT EXISTS idx_fundamentals_active_stock_id ON stock_screener_fundamentals(active_stock_id);
CREATE INDEX IF NOT EXISTS idx_fundamentals_updated_master ON stock_screener_fundamentals(last_updated_at, master_id);
CREATE INDEX IF NOT EXISTS idx_fundamentals_company_trgm ON stock_screener_fundamentals USING GIN (lower(company) gin_trgm_ops);

-- eod: date/range fetches
CREATE INDEX IF NOT EXISTS idx_eod_symbol_exchange ON eod(symbol, exchange);
CREATE INDEX IF NOT EXISTS idx_eod_year_month ON eod(year, month);
CREATE INDEX IF NOT EXISTS idx_eod_master_symbol ON eod(master_id, symbol);

-- tokens: latest/active/expiry checks
CREATE INDEX IF NOT EXISTS idx_tokens_generated_at ON tokens(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_tokens_active_expiry ON tokens(is_active, expiry_time);
