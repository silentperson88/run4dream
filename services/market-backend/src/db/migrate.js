require("dotenv").config();
process.env.PG_AUTO_MIGRATE = "false";
const { pool, dbReady } = require("../config/db");
const { runMigrations } = require("./runMigrations");

dbReady
  .then(() => runMigrations(pool))
  .then(() => {
    console.log("Migrations completed");
  })
  .catch((err) => {
    console.error("Migration failed", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (_) {
      // ignore
    }
  });
