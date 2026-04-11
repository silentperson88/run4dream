const { pool } = require("../config/db");

const getByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_fundamental_overview WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] || null;
};

const getBySymbol = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT o.*
      FROM stock_fundamental_overview o
      INNER JOIN stock_master sm ON sm.id = o.master_id
      WHERE sm.symbol = $1
      LIMIT 1
    `,
    [String(symbol || "").trim()],
  );
  return rows[0] || null;
};

module.exports = {
  getByMasterId,
  getBySymbol,
};
