require("../src/config/env");
const { pool } = require("../src/config/db");
const seedPortfolioTypes = require("./portfolioType.seed");

async function run() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await seedPortfolioTypes(client);
    await client.query("COMMIT");
    console.log("Seeder run completed");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Seeder failed", error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

run();
