require("dotenv").config();

const { dbReady, pool } = require("../config/db");
const {
  createAuditState,
  buildAuditForSnapshot,
  mergeAudit,
  saveAudit,
  finalizeAudit,
} = require("../services/fundamentalsSchemaAudit.service");

const LIMIT = Number(process.env.FUNDAMENTAL_SCHEMA_AUDIT_LIMIT || 0);
const OFFSET = Number(process.env.FUNDAMENTAL_SCHEMA_AUDIT_OFFSET || 0);
const MASTER_ID = Number(process.env.FUNDAMENTAL_SCHEMA_AUDIT_MASTER_ID || 0);

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

const run = async () => {
  console.log(
    `Starting fundamentals schema audit. limit=${LIMIT || "all"}, offset=${OFFSET || 0}, masterId=${MASTER_ID || "all"}`,
  );

  await dbReady;
  const snapshots = await getSnapshots();
  console.log(`Snapshots loaded: ${snapshots.length}`);

  if (!snapshots.length) {
    const emptyAudit = finalizeAudit(createAuditState());
    await saveAudit(emptyAudit);
    console.log(`No snapshots found. Wrote empty audit file to ${emptyAudit.file_path || "audit file"}`);
    return;
  }

  let audit = createAuditState();

  for (let i = 0; i < snapshots.length; i += 1) {
    const snapshot = snapshots[i];
    const stockLabel = snapshot.company || `master_id=${snapshot.master_id}`;
    console.log(`\n[${i + 1}/${snapshots.length}] Auditing ${stockLabel}`);

    const snapshotTables = buildAuditForSnapshot(snapshot);
    const snapshotAudit = {
      tables: snapshotTables,
      totals: { snapshots_scanned: 1, matched_rows: 0, unmatched_rows: 0 },
    };
    snapshotAudit.totals.matched_rows = Object.values(snapshotTables).reduce(
      (sum, bucket) => sum + (bucket.matched_count || 0),
      0,
    );
    snapshotAudit.totals.unmatched_rows = Object.values(snapshotTables).reduce(
      (sum, bucket) => sum + (bucket.unmatched_count || 0),
      0,
    );

    audit = mergeAudit(audit, snapshotAudit);
  }

  const finalized = finalizeAudit(audit);
  await saveAudit(finalized);

  console.log("\nSchema audit completed.");
  console.log({
    snapshots: finalized.totals.snapshots_scanned,
    matched_rows: finalized.totals.matched_rows,
    unmatched_rows: finalized.totals.unmatched_rows,
    file: require("../services/fundamentalsSchemaAudit.service").AUDIT_FILE_PATH,
  });
};

run()
  .catch((error) => {
    console.error("Fatal schema audit error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
