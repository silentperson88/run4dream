require("dotenv").config();
const { pool, dbReady } = require("../config/db");
const { normalizeSymbol } = require("../repositories/eod.repository");

const argv = process.argv.slice(2);
const hasFlag = (name) => argv.includes(`--${name}`) || argv.includes(`--${name}=true`);
const readArg = (name) => {
  const prefix = `--${name}=`;
  const hit = argv.find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
};

const DRY_RUN = hasFlag("dry-run");
const LIMIT = Math.max(0, Number(readArg("limit") || 0) || 0);
const SYMBOL_FILTER = String(readArg("symbol") || "").trim().toUpperCase();
const BATCH_SIZE = Math.max(1, Number(readArg("batch-size") || 500) || 500);

function normalizeKey(symbol) {
  return normalizeSymbol(symbol || "");
}

async function run() {
  console.log("Starting EOD master_id remap by symbol...");
  console.log({
    dryRun: DRY_RUN,
    limit: LIMIT || "all",
    batchSize: BATCH_SIZE,
    symbolFilter: SYMBOL_FILTER || "none",
  });

  await dbReady;
  const client = await pool.connect();

  try {
    const masterResult = await client.query(
      `
        SELECT id, symbol, name, exchange
        FROM stock_master
        WHERE symbol IS NOT NULL
          AND symbol <> ''
          AND is_active = TRUE
      `,
    );

    const masterMap = new Map();
    for (const row of masterResult.rows) {
      const key = normalizeKey(row.symbol);
      if (!key) continue;
      if (!masterMap.has(key)) masterMap.set(key, []);
      masterMap.get(key).push(row);
    }

    const eodParams = [];
    const eodWhere = ["symbol IS NOT NULL", "symbol <> ''"];
    if (SYMBOL_FILTER) {
      eodParams.push(SYMBOL_FILTER);
      eodWhere.push(`UPPER(symbol) = $${eodParams.length}`);
    }

    const eodResult = await client.query(
      `
        SELECT master_id, symbol, trade_date, exchange
        FROM eod
        WHERE ${eodWhere.join(" AND ")}
        ORDER BY trade_date ASC, master_id ASC
      `,
      eodParams,
    );

    const candidates = [];
    const skippedNoMaster = new Map();
    const skippedAmbiguous = new Map();
    const alreadyAligned = [];

    for (const row of eodResult.rows) {
      const currentMasterId = Number(row.master_id);
      const key = normalizeKey(row.symbol);
      const matches = key ? masterMap.get(key) || [] : [];

      if (!matches.length) {
        skippedNoMaster.set(key || String(row.symbol || "unknown"), (skippedNoMaster.get(key || String(row.symbol || "unknown")) || 0) + 1);
        continue;
      }

      if (matches.length > 1) {
        skippedAmbiguous.set(key, (skippedAmbiguous.get(key) || 0) + 1);
        continue;
      }

      const target = matches[0];
      if (currentMasterId === Number(target.id)) {
        alreadyAligned.push(row);
        continue;
      }

      candidates.push({
        old_master_id: currentMasterId,
        new_master_id: Number(target.id),
        symbol: row.symbol,
        trade_date: row.trade_date,
        exchange: row.exchange,
        target_symbol: target.symbol,
        target_name: target.name,
        target_exchange: target.exchange,
      });
    }

    const limitedCandidates = LIMIT > 0 ? candidates.slice(0, LIMIT) : candidates;
    console.log(`Loaded ${eodResult.rows.length} EOD rows`);
    console.log(`Aligned already: ${alreadyAligned.length}`);
    console.log(`Candidates to remap: ${limitedCandidates.length}`);
    console.log(`Missing symbol matches: ${Array.from(skippedNoMaster.values()).reduce((a, b) => a + b, 0)}`);
    console.log(`Ambiguous symbol matches: ${Array.from(skippedAmbiguous.values()).reduce((a, b) => a + b, 0)}`);

    if (!limitedCandidates.length) {
      console.log("No EOD rows need remapping.");
      return;
    }

    let updated = 0;
    let skippedCollision = 0;
    let failed = 0;
    let processed = 0;
    const affectedMasterIds = new Set();

    await client.query("BEGIN");
    try {
      for (let i = 0; i < limitedCandidates.length; i += BATCH_SIZE) {
        const batch = limitedCandidates.slice(i, i + BATCH_SIZE);
        for (const row of batch) {
          processed += 1;

          const collisionCheck = await client.query(
            `
              SELECT 1
              FROM eod
              WHERE master_id = $1
                AND trade_date = $2::date
              LIMIT 1
            `,
            [row.new_master_id, row.trade_date],
          );

          if (collisionCheck.rowCount > 0) {
            skippedCollision += 1;
            continue;
          }

          if (DRY_RUN) {
            updated += 1;
            affectedMasterIds.add(row.new_master_id);
            continue;
          }

          const updateResult = await client.query(
            `
              UPDATE eod
              SET master_id = $1,
                  updated_at = NOW()
              WHERE master_id = $2
                AND trade_date = $3::date
            `,
            [row.new_master_id, row.old_master_id, row.trade_date],
          );

          if (updateResult.rowCount > 0) {
            updated += updateResult.rowCount;
            affectedMasterIds.add(row.new_master_id);
          } else {
            failed += 1;
          }
        }
      }

      if (affectedMasterIds.size > 0) {
        const affectedIds = Array.from(affectedMasterIds);
        const historyRes = await client.query(
          `
            SELECT
              master_id,
              MIN(trade_date)::date AS history_from_date,
              MAX(trade_date)::date AS history_to_date
            FROM eod
            WHERE master_id = ANY($1::bigint[])
            GROUP BY master_id
          `,
          [affectedIds],
        );

        for (const row of historyRes.rows) {
          if (DRY_RUN) continue;

          await client.query(
            `
              UPDATE stock_master
              SET
                history_range = CASE
                  WHEN $2::date IS NULL AND $3::date IS NULL THEN history_range
                  WHEN $2::date IS NULL THEN to_char($3::date, 'YYYY-MM-DD')
                  WHEN $3::date IS NULL THEN to_char($2::date, 'YYYY-MM-DD')
                  ELSE to_char($2::date, 'YYYY-MM-DD') || ' to ' || to_char($3::date, 'YYYY-MM-DD')
                END,
                updated_at = NOW()
              WHERE id = $1
            `,
            [Number(row.master_id), row.history_from_date, row.history_to_date],
          );
        }
      }

      if (DRY_RUN) {
        await client.query("ROLLBACK");
      } else {
        await client.query("COMMIT");
      }
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    }

    console.log("EOD master_id remap completed.");
    console.log({
      processed,
      updated,
      skippedCollision,
      failed,
      dryRun: DRY_RUN,
    });
  } finally {
    client.release();
    await pool.end().catch(() => {});
  }
}

run().catch((err) => {
  console.error("EOD remap failed:", err?.message || err);
  process.exit(1);
});
