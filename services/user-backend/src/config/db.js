const { Client, Pool } = require("pg");
const { runMigrations } = require("../db/runMigrations");

const DATABASE_URL = process.env.PG_DATABASE_URL;
if (!DATABASE_URL) throw new Error("PG_DATABASE_URL is required");

const AUTO_CREATE_DB = (process.env.PG_AUTO_CREATE_DB || "true").toLowerCase() === "true";
const SSL = process.env.PG_SSL === "true" ? { rejectUnauthorized: false } : false;

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: SSL,
});

const quoteIdent = (ident) => `"${String(ident).replace(/"/g, "\"\"")}"`;

const ensureDatabaseExists = async () => {
  if (!AUTO_CREATE_DB) return;

  const url = new URL(DATABASE_URL);
  const dbName = decodeURIComponent((url.pathname || "").replace(/^\//, ""));
  if (!dbName) return;

  const adminUrl = new URL(DATABASE_URL);
  adminUrl.pathname = "/postgres";

  const adminClient = new Client({
    connectionString: adminUrl.toString(),
    ssl: SSL,
  });

  await adminClient.connect();
  try {
    const exists = await adminClient.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [dbName],
    );
    if (!exists.rowCount) {
      await adminClient.query(`CREATE DATABASE ${quoteIdent(dbName)}`);
      console.log(`User-backend PostgreSQL database created: ${dbName}`);
    }
  } finally {
    await adminClient.end();
  }
};

const dbReady = (async () => {
  await ensureDatabaseExists();
  await pool.query("SELECT 1");
  console.log("User-backend PostgreSQL connected");
  if ((process.env.PG_AUTO_MIGRATE || "false").toLowerCase() === "true") {
    await runMigrations(pool);
  }
})().catch((err) => {
  console.error("User-backend PostgreSQL init failed", err?.message || err);
  throw err;
});

module.exports = { pool, dbReady };
