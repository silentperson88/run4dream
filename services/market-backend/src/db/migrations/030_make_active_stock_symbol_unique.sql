DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'active_stock_name_key'
  ) THEN
    ALTER TABLE active_stock DROP CONSTRAINT active_stock_name_key;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'active_stock_symbol_key'
  ) THEN
    ALTER TABLE active_stock DROP CONSTRAINT active_stock_symbol_key;
  END IF;
END $$;
