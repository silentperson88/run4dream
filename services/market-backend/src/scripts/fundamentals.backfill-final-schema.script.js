require("dotenv").config();

const { dbReady, pool } = require("../config/db");
const {
  loadAudit,
  buildFinalSchemaFromSelection,
  saveFinalSchema,
} = require("../services/fundamentalsSchemaAudit.service");
const {
  buildStructuredPayload,
  upsertStructuredRow,
} = require("../services/fundamentalsStructuredBackfill.service");

const DRY_RUN = String(process.env.FUNDAMENTAL_FINAL_BACKFILL_DRY_RUN || "false").toLowerCase() === "true";
const LIMIT = Number(process.env.FUNDAMENTAL_FINAL_BACKFILL_LIMIT || 0);
const OFFSET = Number(process.env.FUNDAMENTAL_FINAL_BACKFILL_OFFSET || 0);
const MASTER_ID = Number(process.env.FUNDAMENTAL_FINAL_BACKFILL_MASTER_ID || 0);

const FINAL_TABLE_KEYS = new Set([
  "company_overview",
  "quarterly_results",
  "profit_loss",
  "balance_sheet",
  "cash_flow",
  "ratios",
  "shareholdings",
]);

const PRIMARY_OVERVIEW_FIELDS = ["market_cap", "current_price", "book_value"];

const ensureQualityColumns = async (db = pool) => {
  await db.query(`
    ALTER TABLE stock_master
      ADD COLUMN IF NOT EXISTS fundamentals_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
      ADD COLUMN IF NOT EXISTS fundamentals_checked_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS fundamentals_failed_fields TEXT[],
      ADD COLUMN IF NOT EXISTS fundamentals_failed_reason TEXT
  `);

  await db.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_stock_master_fundamentals_status'
      ) THEN
        ALTER TABLE stock_master
          ADD CONSTRAINT chk_stock_master_fundamentals_status
          CHECK (fundamentals_status IN ('PENDING', 'VALID', 'PARTIAL', 'FAILED'));
      END IF;
    END $$;
  `);
};

const getSnapshots = async () => {
  const params = [];
  const where = [];

  if (Number.isFinite(MASTER_ID) && MASTER_ID > 0) {
    params.push(MASTER_ID);
    where.push(`master_id = $${params.length}`);
  }

  let sql = `SELECT * FROM stock_screener_fundamentals`;
  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += ` ORDER BY master_id ASC, id ASC`;
  if (Number.isFinite(LIMIT) && LIMIT > 0) {
    params.push(LIMIT);
    sql += ` LIMIT $${params.length}`;
  }
  if (Number.isFinite(OFFSET) && OFFSET > 0) {
    params.push(OFFSET);
    sql += ` OFFSET $${params.length}`;
  }

  const { rows } = await pool.query(sql, params);
  return rows;
};

const isUsableNumber = (value) => {
  if (value === null || value === undefined) return false;
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  const parsed = Number(String(value).replace(/,/g, "").replace(/₹/g, "").trim());
  return Number.isFinite(parsed) && parsed > 0;
};

const pickAllowedKeys = (row, keys = []) => {
  const out = {};
  keys.forEach((key) => {
    if (row[key] !== undefined) {
      out[key] = row[key];
    }
  });
  return out;
};

const tableMetadataKeys = (tableKey) => {
  if (tableKey === "company_overview") {
    return [
      "master_id",
      "active_stock_id",
      "snapshot_id",
      "last_updated_at",
      "updated_at",
    ];
  }

  const keys = [
    "master_id",
    "active_stock_id",
    "snapshot_id",
    "period_label",
    "period_end",
    "period_index",
    "title",
    "headers",
    "raw_row",
    "row_label",
    "last_updated_at",
    "updated_at",
  ];

  if (tableKey === "shareholdings") {
    keys.push("children");
  }

  return keys;
};

const buildRowsForTable = (tableKey, bucket, payload, snapshot) => {
  const selectedKeys = (bucket?.selected_columns || []).map((column) => column.key);
  const allowedKeys = new Set([...tableMetadataKeys(tableKey), ...selectedKeys]);

  if (tableKey === "company_overview") {
    const row = {
      ...pickAllowedKeys(payload.overview || {}, Array.from(allowedKeys)),
      master_id: Number(snapshot.master_id),
      active_stock_id: Number(snapshot.active_stock_id) || null,
      snapshot_id: snapshot.id,
      last_updated_at: snapshot.last_updated_at || snapshot.updated_at || new Date(),
      updated_at: new Date(),
    };
    return [row];
  }

  const sourceRows = Array.isArray(payload[tableKey]) ? payload[tableKey] : [];
  return sourceRows.map((sourceRow) => ({
    ...pickAllowedKeys(sourceRow || {}, Array.from(allowedKeys)),
    master_id: Number(snapshot.master_id),
    active_stock_id: Number(snapshot.active_stock_id) || null,
    snapshot_id: snapshot.id,
    last_updated_at: snapshot.last_updated_at || snapshot.updated_at || new Date(),
    updated_at: new Date(),
  }));
};

const updateStockMasterStatus = async (client, snapshot, overviewRow) => {
  const missing = PRIMARY_OVERVIEW_FIELDS.filter((field) => !isUsableNumber(overviewRow?.[field]));
  const status = missing.length ? "FAILED" : "VALID";
  const reason = missing.length
    ? `Missing/invalid primary fields: ${missing.join(", ")}`
    : null;

  await client.query(
    `
      UPDATE stock_master
      SET
        fundamentals_status = $2,
        fundamentals_checked_at = NOW(),
        fundamentals_failed_fields = $3::text[],
        fundamentals_failed_reason = $4,
        updated_at = NOW()
      WHERE id = $1
    `,
    [
      Number(snapshot.master_id),
      status,
      missing.length ? missing : null,
      reason,
    ],
  );

  return { status, missing, reason };
};

const run = async () => {
  console.log(
    `Starting final schema backfill. dryRun=${DRY_RUN}, limit=${LIMIT || "all"}, offset=${OFFSET || 0}, masterId=${MASTER_ID || "all"}`,
  );

  await dbReady;
  await ensureQualityColumns(pool);
  const audit = await loadAudit();
  const finalSchemaSnapshot = buildFinalSchemaFromSelection(audit, {});
  await saveFinalSchema(finalSchemaSnapshot);
  const finalSchema = Object.fromEntries(
    Object.entries(finalSchemaSnapshot?.tables || {}).filter(([tableKey]) => FINAL_TABLE_KEYS.has(tableKey)),
  );
  const snapshots = await getSnapshots();

  console.log(`Final schema tables loaded: ${Object.keys(finalSchema).join(", ")}`);
  console.log(`Snapshots loaded: ${snapshots.length}`);

  if (!snapshots.length) {
    console.log("No snapshots found in stock_screener_fundamentals.");
    return;
  }

  let success = 0;
  let failed = 0;
  let failedStocks = 0;

  for (let i = 0; i < snapshots.length; i += 1) {
    const snapshot = snapshots[i];
    const label = snapshot.company || `master_id=${snapshot.master_id}`;
    console.log(`\n[${i + 1}/${snapshots.length}] Backfilling ${label}`);

    try {
      const payload = buildStructuredPayload(snapshot);
      const overviewRow = payload.overview || {};

      if (DRY_RUN) {
        const status = PRIMARY_OVERVIEW_FIELDS.every((field) => isUsableNumber(overviewRow?.[field]))
          ? "VALID"
          : "FAILED";
        console.log(
          `Dry run -> status=${status}, quarterly=${(payload.quarterly_results || []).length}, profit_loss=${(payload.profit_loss || []).length}, balance_sheet=${(payload.balance_sheet || []).length}, cash_flow=${(payload.cash_flow || []).length}, ratios=${(payload.ratios || []).length}, shareholdings=${(payload.shareholdings || []).length}`,
        );
        success += 1;
        if (status === "FAILED") failedStocks += 1;
        continue;
      }

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const overviewValidation = await updateStockMasterStatus(client, snapshot, overviewRow);
        if (overviewValidation.status === "FAILED") failedStocks += 1;

        for (const [tableKey, bucket] of Object.entries(finalSchema)) {
          const rows = buildRowsForTable(tableKey, bucket, payload, snapshot);
          const tableName = bucket?.table;
          if (!tableName || !rows.length) continue;

          const conflictColumns =
            tableKey === "company_overview"
              ? ["master_id"]
              : ["master_id", "period_label"];

          for (const row of rows) {
            // eslint-disable-next-line no-await-in-loop
            await upsertStructuredRow(client, tableName, row, conflictColumns);
          }
        }

        await client.query("COMMIT");
        success += 1;
        console.log(
          `Done ${label} -> status=${overviewValidation.status}, quarterly=${(payload.quarterly_results || []).length}, profit_loss=${(payload.profit_loss || []).length}, balance_sheet=${(payload.balance_sheet || []).length}, cash_flow=${(payload.cash_flow || []).length}, ratios=${(payload.ratios || []).length}, shareholdings=${(payload.shareholdings || []).length}`,
        );
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    } catch (error) {
      failed += 1;
      const message =
        error?.response?.data?.message ||
        error?.response?.data?.error?.message ||
        error?.message ||
        "Unknown error";
      console.error(`[${i + 1}/${snapshots.length}] Failed master_id=${snapshot.master_id}: ${message}`);
    }
  }

  console.log("\nFinal schema backfill completed.");
  console.log({
    total: snapshots.length,
    success,
    failed,
    failedStocks,
    dryRun: DRY_RUN,
    limit: LIMIT || null,
    offset: OFFSET || null,
    masterId: MASTER_ID || null,
  });
};

run()
  .catch((error) => {
    console.error("Fatal final schema backfill error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
