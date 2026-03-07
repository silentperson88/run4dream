CREATE INDEX IF NOT EXISTS idx_orders_user_status_created
  ON orders(user_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_user_portfolio_stock_created
  ON orders(user_id, portfolio_id, active_stock_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_open_partial_created
  ON orders(status, created_at)
  WHERE status IN ('OPEN', 'PARTIALLY_FILLED');

CREATE INDEX IF NOT EXISTS idx_user_portfolios_user_status_created
  ON user_portfolios(user_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_active_stock_symbol_active
  ON active_stock(symbol, is_active);
