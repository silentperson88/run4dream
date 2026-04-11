const { getPool, ensureSchema: ensureContentSchema } = require("./newsIngest.db");

let schemaInitPromise = null;

async function ensureSchema() {
  if (schemaInitPromise) return schemaInitPromise;
  schemaInitPromise = (async () => {
    await ensureContentSchema();
    const db = getPool();

    await db.query(
      `
        CREATE TABLE IF NOT EXISTS social_account_connections (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          platform TEXT NOT NULL,
          account_label TEXT NOT NULL DEFAULT '',
          is_connected BOOLEAN NOT NULL DEFAULT FALSE,
          connection_data JSONB NOT NULL DEFAULT '{}'::jsonb,
          notes TEXT,
          last_verified_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (user_id, platform)
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_social_account_connections_user_id ON social_account_connections(user_id, platform);",
    );
    await db.query("ALTER TABLE social_account_connections ADD COLUMN IF NOT EXISTS notes TEXT;");
    await db.query(
      "ALTER TABLE social_account_connections ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ;",
    );
    await db.query(
      "ALTER TABLE social_account_connections ADD COLUMN IF NOT EXISTS is_connected BOOLEAN NOT NULL DEFAULT FALSE;",
    );
    await db.query(
      "ALTER TABLE social_account_connections ADD COLUMN IF NOT EXISTS connection_data JSONB NOT NULL DEFAULT '{}'::jsonb;",
    );
    await db.query(
      "ALTER TABLE social_account_connections ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();",
    );
  })();
  return schemaInitPromise;
}

module.exports = {
  getPool,
  ensureSchema,
};
