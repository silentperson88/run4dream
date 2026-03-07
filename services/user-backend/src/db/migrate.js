require("dotenv").config();
process.env.PG_AUTO_MIGRATE = "false";
const { pool, dbReady } = require("../config/db");
const { runMigrations } = require("./runMigrations");

dbReady
  .then(() => runMigrations(pool))
  .then(() => console.log("User-backend migrations completed"))
  .catch((err) => {
    console.error("User-backend migration failed", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (_) {}
  });
