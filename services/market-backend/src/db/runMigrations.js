const fs = require("fs/promises");
const path = require("path");
const ensureMigrationTable = async (pool) => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      id BIGSERIAL PRIMARY KEY,
      filename TEXT NOT NULL UNIQUE,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
};

const getAppliedMigrations = async (pool) => {
  const { rows } = await pool.query(`SELECT filename FROM schema_migrations`);
  return new Set(rows.map((r) => r.filename));
};

const applyMigrationFile = async (pool, filename, sql) => {
  await pool.query("BEGIN");
  try {
    await pool.query(sql);
    await pool.query(
      `INSERT INTO schema_migrations (filename, applied_at) VALUES ($1, NOW())`,
      [filename],
    );
    await pool.query("COMMIT");
    console.log(`Applied migration: ${filename}`);
  } catch (err) {
    await pool.query("ROLLBACK");
    throw err;
  }
};

const runMigrations = async (pool) => {
  const migrationsDir = path.join(__dirname, "migrations");
  await ensureMigrationTable(pool);

  const files = (await fs.readdir(migrationsDir))
    .filter((f) => f.endsWith(".sql"))
    .sort();

  const applied = await getAppliedMigrations(pool);
  for (const file of files) {
    if (applied.has(file)) continue;
    const sqlRaw = await fs.readFile(path.join(migrationsDir, file), "utf8");
    const sql = sqlRaw.replace(/^\uFEFF/, "");
    await applyMigrationFile(pool, file, sql);
  }
};

module.exports = { runMigrations };
