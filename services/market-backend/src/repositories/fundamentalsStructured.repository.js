const { pool } = require("../config/db");
const { upsertStructuredRow } = require("../services/fundamentalsStructuredBackfill.service");

const STRUCTURED_TABLES = {
  overview: "stock_fundamental_overview",
  peers: "stock_fundamental_peers_snapshot",
  quarterly_results: "stock_fundamental_quarterly_results",
  profit_loss: "stock_fundamental_profit_loss_periods",
  balance_sheet: "stock_fundamental_balance_sheet_periods",
  cash_flow: "stock_fundamental_cash_flow_periods",
  ratios: "stock_fundamental_ratios_periods",
  shareholdings: "stock_fundamental_shareholdings_periods",
};

const assertKnownTable = (sectionKey) => {
  const tableName = STRUCTURED_TABLES[sectionKey];
  if (!tableName) {
    throw new Error(`Unknown structured fundamentals section: ${sectionKey}`);
  }
  return tableName;
};

const upsertOverview = async (row, db = pool) =>
  upsertStructuredRow(db, STRUCTURED_TABLES.overview, row, ["master_id"]);

const upsertPeersSnapshot = async (row, db = pool) =>
  upsertStructuredRow(db, STRUCTURED_TABLES.peers, row, ["master_id"]);

const upsertSectionRows = async (sectionKey, rows = [], db = pool) => {
  const tableName = assertKnownTable(sectionKey);
  const conflictColumns = ["master_id", "period_label"];
  const results = [];

  for (const row of rows) {
    // eslint-disable-next-line no-await-in-loop
    results.push(await upsertStructuredRow(db, tableName, row, conflictColumns));
  }

  return results;
};

const getOverviewByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM ${STRUCTURED_TABLES.overview} WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] || null;
};

const getPeersSnapshotByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM ${STRUCTURED_TABLES.peers} WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] || null;
};

const getSectionRowsByMasterId = async (sectionKey, masterId, db = pool) => {
  const tableName = assertKnownTable(sectionKey);
  const { rows } = await db.query(
    `SELECT * FROM ${tableName} WHERE master_id = $1 ORDER BY period_index ASC NULLS LAST, period_end ASC NULLS LAST, id ASC`,
    [Number(masterId)],
  );
  return rows;
};

const getStructuredFundamentalsByMasterId = async (masterId, db = pool) => {
  const [overview, peers, quarterlyResults, profitLoss, balanceSheet, cashFlow, ratios, shareholdings] =
    await Promise.all([
      getOverviewByMasterId(masterId, db),
      getPeersSnapshotByMasterId(masterId, db),
      getSectionRowsByMasterId("quarterly_results", masterId, db),
      getSectionRowsByMasterId("profit_loss", masterId, db),
      getSectionRowsByMasterId("balance_sheet", masterId, db),
      getSectionRowsByMasterId("cash_flow", masterId, db),
      getSectionRowsByMasterId("ratios", masterId, db),
      getSectionRowsByMasterId("shareholdings", masterId, db),
    ]);

  return {
    overview,
    peers,
    quarterly_results: quarterlyResults,
    profit_loss: profitLoss,
    balance_sheet: balanceSheet,
    cash_flow: cashFlow,
    ratios,
    shareholdings,
  };
};

module.exports = {
  STRUCTURED_TABLES,
  upsertOverview,
  upsertPeersSnapshot,
  upsertSectionRows,
  getOverviewByMasterId,
  getPeersSnapshotByMasterId,
  getSectionRowsByMasterId,
  getStructuredFundamentalsByMasterId,
};
