const axios = require("axios");
require("dotenv").config();
const { pool, dbReady } = require("../config/db");

async function insertRawStocks() {
  try {
    await dbReady;

    const response = await axios.get(
      "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
    );
    const rawSymbols = response.data;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const s of rawSymbols) {
        await client.query(
          `
            INSERT INTO rawstocks (
              token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,'pending')
            ON CONFLICT (token)
            DO UPDATE SET
              symbol = EXCLUDED.symbol,
              name = EXCLUDED.name,
              exch_seg = EXCLUDED.exch_seg,
              instrumenttype = EXCLUDED.instrumenttype,
              lotsize = EXCLUDED.lotsize,
              tick_size = EXCLUDED.tick_size
          `,
          [
            s.token,
            s.symbol,
            s.name,
            s.exch_seg || s.exchange,
            s.instrumenttype || "EQ",
            Number(s.lotsize) || 1,
            Number(s.tick_size) || null,
          ],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    console.log("Raw stocks inserted successfully");
    process.exit(0);
  } catch (err) {
    console.error("Error inserting raw stocks", err);
    process.exit(1);
  }
}

insertRawStocks();
