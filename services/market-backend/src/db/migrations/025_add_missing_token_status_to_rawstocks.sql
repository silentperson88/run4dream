ALTER TABLE IF EXISTS rawstocks
  DROP CONSTRAINT IF EXISTS rawstocks_status_check;

ALTER TABLE IF EXISTS rawstocks
  ADD CONSTRAINT rawstocks_status_check
  CHECK (status IN ('pending', 'missing_token', 'approved', 'rejected'));

UPDATE rawstocks
SET status = 'missing_token'
WHERE (token IS NULL OR trim(token) = '')
  AND status = 'pending';
