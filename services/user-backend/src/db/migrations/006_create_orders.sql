CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  portfolio_id BIGINT NOT NULL REFERENCES user_portfolios(id) ON DELETE CASCADE,
  active_stock_id BIGINT NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  exchange VARCHAR(16) NOT NULL,
  type VARCHAR(8) NOT NULL,
  order_type VARCHAR(16) NOT NULL,
  order_price NUMERIC(18,6) NOT NULL,
  order_quantity INTEGER NOT NULL,
  executed_quantity INTEGER NOT NULL DEFAULT 0,
  remaining_quantity INTEGER NOT NULL,
  avg_execution_price NUMERIC(18,6) NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
  executions JSONB NOT NULL DEFAULT '[]'::jsonb,
  realized_pl NUMERIC(18,6) NOT NULL DEFAULT 0,
  max_partial_executions INTEGER NOT NULL DEFAULT 5,
  sell_allocation JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (type IN ('BUY','SELL')),
  CHECK (order_type IN ('MARKET','LIMIT')),
  CHECK (status IN ('OPEN','PARTIALLY_FILLED','COMPLETED','CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_active_type ON orders(active_stock_id, type);
CREATE INDEX IF NOT EXISTS idx_orders_portfolio_status ON orders(portfolio_id, status);
