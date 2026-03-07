const { pool } = require("../config/db");

async function connectDB() {
  await pool.query("SELECT 1");
  return pool;
}

module.exports = { connectDB };
