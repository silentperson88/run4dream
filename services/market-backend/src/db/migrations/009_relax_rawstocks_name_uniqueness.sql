ALTER TABLE rawstocks
  DROP CONSTRAINT IF EXISTS rawstocks_name_key;

CREATE INDEX IF NOT EXISTS idx_rawstocks_name
  ON rawstocks(name);
