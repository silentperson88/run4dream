require("dotenv").config();

const { dbReady, pool } = require("../config/db");
const { buildStructuredPayload, upsertStructuredRow } = require("../services/fundamentalsStructuredBackfill.service");

const DRY_RUN = String(process.env.FUNDAMENTAL_BACKFILL_DRY_RUN || "false").toLowerCase() === "true";
const LIMIT = Number(process.env.FUNDAMENTAL_BACKFILL_LIMIT || 0);
const OFFSET = Number(process.env.FUNDAMENTAL_BACKFILL_OFFSET || 0);
const MASTER_ID = Number(process.env.FUNDAMENTAL_BACKFILL_MASTER_ID || 0);

const sectionConfig = [
  {
    key: "overview",
    table: "stock_fundamental_overview",
    conflictColumns: ["master_id"],
  },
  {
    key: "peers",
    table: "stock_fundamental_peers_snapshot",
    conflictColumns: ["master_id"],
  },
  {
    key: "quarterly_results",
    table: "stock_fundamental_quarterly_results",
    conflictColumns: ["master_id", "period_label"],
  },
  {
    key: "profit_loss",
    table: "stock_fundamental_profit_loss_periods",
    conflictColumns: ["master_id", "period_label"],
  },
  {
    key: "balance_sheet",
    table: "stock_fundamental_balance_sheet_periods",
    conflictColumns: ["master_id", "period_label"],
  },
  {
    key: "cash_flow",
    table: "stock_fundamental_cash_flow_periods",
    conflictColumns: ["master_id", "period_label"],
  },
  {
    key: "ratios",
    table: "stock_fundamental_ratios_periods",
    conflictColumns: ["master_id", "period_label"],
  },
  {
    key: "shareholdings",
    table: "stock_fundamental_shareholdings_periods",
    conflictColumns: ["master_id", "period_label"],
  },
];

const getSnapshots = async () => {
  const params = [];
  const where = [];

  if (Number.isFinite(MASTER_ID) && MASTER_ID > 0) {
    params.push(MASTER_ID);
    where.push(`master_id = $${params.length}`);
  }

  let sql = `
    SELECT *
    FROM stock_screener_fundamentals
  `;
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

const runForSnapshot = async (snapshot, index, total) => {
  const masterId = Number(snapshot.master_id);
  const activeStockId = Number(snapshot.active_stock_id);
  const label = snapshot.company || `master_id=${masterId}`;

  console.log(`\n[${index + 1}/${total}] Backfilling ${label} (master_id=${masterId})`);

  const payload = buildStructuredPayload(snapshot);
  if (DRY_RUN) {
    console.log(
      `Dry run -> overview=yes, peers=yes, quarterly=${payload.quarterly_results.length}, profit_loss=${payload.profit_loss.length}, balance_sheet=${payload.balance_sheet.length}, cash_flow=${payload.cash_flow.length}, ratios=${payload.ratios.length}, shareholdings=${payload.shareholdings.length}`,
    );
    return {
      masterId,
      dryRun: true,
    };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await upsertStructuredRow(
      client,
      "stock_fundamental_overview",
      {
        master_id: masterId,
        active_stock_id: activeStockId,
        snapshot_id: snapshot.id,
        ...payload.overview,
        last_updated_at: snapshot.last_updated_at || snapshot.updated_at || new Date(),
        updated_at: new Date(),
      },
      ["master_id"],
    );

    await upsertStructuredRow(
      client,
      "stock_fundamental_peers_snapshot",
      {
        master_id: masterId,
        active_stock_id: activeStockId,
        snapshot_id: snapshot.id,
        ...payload.peers,
        last_updated_at: snapshot.last_updated_at || snapshot.updated_at || new Date(),
        updated_at: new Date(),
      },
      ["master_id"],
    );

    for (const cfg of sectionConfig.slice(2)) {
      const rows = payload[cfg.key] || [];
      for (const row of rows) {
        await upsertStructuredRow(
          client,
          cfg.table,
          {
            master_id: masterId,
            active_stock_id: activeStockId,
            snapshot_id: snapshot.id,
            ...row,
            updated_at: new Date(),
          },
          cfg.conflictColumns,
        );
      }
    }

    await client.query("COMMIT");
    console.log(
      `Done ${label}: quarterly=${payload.quarterly_results.length}, profit_loss=${payload.profit_loss.length}, balance_sheet=${payload.balance_sheet.length}, cash_flow=${payload.cash_flow.length}, ratios=${payload.ratios.length}, shareholdings=${payload.shareholdings.length}`,
    );

    return {
      masterId,
      dryRun: false,
      rows: payload,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

const run = async () => {
  console.log(
    `Starting structured fundamentals backfill. dryRun=${DRY_RUN}, limit=${LIMIT || "all"}, offset=${OFFSET || 0}, masterId=${MASTER_ID || "all"}`,
  );

  await dbReady;
  const snapshots = await getSnapshots();
  console.log(`Snapshots loaded: ${snapshots.length}`);

  if (!snapshots.length) {
    console.log("No snapshot rows found in stock_screener_fundamentals.");
    return;
  }

  let success = 0;
  let failed = 0;

  for (let i = 0; i < snapshots.length; i += 1) {
    const snapshot = snapshots[i];
    try {
      await runForSnapshot(snapshot, i, snapshots.length);
      success += 1;
    } catch (error) {
      failed += 1;
      const message =
        error?.response?.data?.message ||
        error?.response?.data?.error?.message ||
        error?.message ||
        "Unknown error";
      console.error(
        `[${i + 1}/${snapshots.length}] Failed master_id=${snapshot.master_id}: ${message}`,
      );
    }
  }

  console.log("\nStructured fundamentals backfill completed.");
  console.log({
    total: snapshots.length,
    success,
    failed,
    dryRun: DRY_RUN,
    limit: LIMIT || null,
    offset: OFFSET || null,
    masterId: MASTER_ID || null,
  });
};

run()
  .catch((error) => {
    console.error("Fatal backfill error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
