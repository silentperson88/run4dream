ALTER TABLE IF EXISTS rawstocks
  ADD COLUMN IF NOT EXISTS security_code VARCHAR(64);

ALTER TABLE IF EXISTS stock_master
  ADD COLUMN IF NOT EXISTS security_code VARCHAR(64);

ALTER TABLE IF EXISTS active_stock
  ADD COLUMN IF NOT EXISTS security_code VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_rawstocks_security_code ON rawstocks(security_code);
CREATE INDEX IF NOT EXISTS idx_stock_master_security_code ON stock_master(security_code);
CREATE INDEX IF NOT EXISTS idx_active_stock_security_code ON active_stock(security_code);
