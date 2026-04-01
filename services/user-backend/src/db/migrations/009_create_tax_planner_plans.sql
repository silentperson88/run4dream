CREATE TABLE IF NOT EXISTS tax_planner_plans (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name VARCHAR(160) NOT NULL DEFAULT 'Default',
  buy_lots JSONB NOT NULL DEFAULT '[]'::jsonb,
  sell_trades JSONB NOT NULL DEFAULT '[]'::jsonb,
  settings JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_tax_planner_plans_user_name UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_tax_planner_plans_user_updated
  ON tax_planner_plans(user_id, updated_at DESC);
