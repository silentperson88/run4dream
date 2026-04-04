DO $$
DECLARE
  seq_name text;
BEGIN
  SELECT pg_get_serial_sequence('tokens', 'id') INTO seq_name;
  IF seq_name IS NOT NULL THEN
    EXECUTE format(
      'SELECT setval(%L, COALESCE((SELECT MAX(id) FROM tokens), 0) + 1, false)',
      seq_name
    );
  END IF;
END $$;
