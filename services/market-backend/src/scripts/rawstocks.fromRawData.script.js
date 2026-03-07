require("dotenv").config();
const fs = require("fs/promises");
const path = require("path");
const { pool, dbReady } = require("../config/db");

const RAW_DATA_DIR = path.resolve(__dirname, "../../rawData");
const BATCH_SIZE = Number(process.env.RAWSTOCKS_SEED_BATCH_SIZE || 2000);

const toInt = (value, fallback = 1) => {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
};

const toNullableNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeRecord = (record = {}) => {
  const token = String(record.token || "").trim();
  const symbol = String(record.symbol || "").trim();
  const name = String(record.name || "").trim();
  const exchSeg = String(record.exch_seg || record.exchange || "").trim();

  if (!token || !symbol || !name || !exchSeg) return null;

  return {
    token,
    symbol,
    name,
    exch_seg: exchSeg,
    instrumenttype: String(record.instrumenttype || "EQ").trim() || "EQ",
    lotsize: toInt(record.lotsize, 1),
    tick_size: toNullableNumber(record.tick_size),
    status: "pending",
  };
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
};

const UPSERT_COLUMNS = [
  "token",
  "symbol",
  "name",
  "exch_seg",
  "instrumenttype",
  "lotsize",
  "tick_size",
  "status",
];

const upsertBatch = async (client, rows) => {
  if (!rows.length) return;

  const values = [];
  const tuples = rows.map((r, rowIdx) => {
    const offset = rowIdx * UPSERT_COLUMNS.length;
    values.push(
      r.token,
      r.symbol,
      r.name,
      r.exch_seg,
      r.instrumenttype,
      r.lotsize,
      r.tick_size,
      r.status,
    );
    return `($${offset + 1},$${offset + 2},$${offset + 3},$${offset + 4},$${offset + 5},$${offset + 6},$${offset + 7},$${offset + 8})`;
  });

  await client.query(
    `
      INSERT INTO rawstocks (
        token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status
      )
      VALUES ${tuples.join(",")}
      ON CONFLICT (token)
      DO UPDATE SET
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        exch_seg = EXCLUDED.exch_seg,
        instrumenttype = EXCLUDED.instrumenttype,
        lotsize = EXCLUDED.lotsize,
        tick_size = EXCLUDED.tick_size,
        status = EXCLUDED.status
    `,
    values,
  );
};

const readJsonFile = async (filePath) => {
  const text = await fs.readFile(filePath, "utf8");
  const parsed = JSON.parse(text);
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === "object") return [parsed];
  return [];
};

async function seedFromRawDataFiles() {
  await dbReady;

  const files = (await fs.readdir(RAW_DATA_DIR))
    .filter((f) => f.toLowerCase().endsWith(".json"))
    .map((f) => path.join(RAW_DATA_DIR, f));

  if (!files.length) {
    throw new Error(`No JSON files found in ${RAW_DATA_DIR}`);
  }

  const client = await pool.connect();

  try {
    let totalProcessed = 0;

    for (const file of files) {
      const rows = await readJsonFile(file);
      const normalized = rows.map(normalizeRecord).filter(Boolean);

      if (!normalized.length) continue;

      const batches = chunk(normalized, BATCH_SIZE);

      for (const batch of batches) {
        await upsertBatch(client, batch);
        totalProcessed += batch.length;
        console.log(
          `Processed ${totalProcessed} rows (current file: ${path.basename(file)})`,
        );
      }
    }
    console.log(`Rawstocks seeded successfully: ${totalProcessed}`);
  } catch (err) {
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

seedFromRawDataFiles().catch((err) => {
  console.error("Raw data seed failed", err);
  process.exit(1);
});
