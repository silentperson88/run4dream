const { pool } = require("../config/db");
const { ensureObject, syncSerialSequence } = require("./common");

const SECTION_COLUMNS = [
  "quarters_table",
  "profit_loss_table",
  "balance_sheet_table",
  "cash_flow_table",
  "ratios_table",
  "shareholdings_table",
];

const extractSectionTables = (row = {}) => {
  return {
    quarters: ensureObject(row.quarters_table),
    profit_loss: ensureObject(row.profit_loss_table),
    balance_sheet: ensureObject(row.balance_sheet_table),
    cash_flow: ensureObject(row.cash_flow_table),
    ratios: ensureObject(row.ratios_table),
    shareholdings: ensureObject(row.shareholdings_table),
  };
};

const normalizeFundamental = (row = {}) => {
  const normalized = {
    ...row,
    company_info: ensureObject(row.company_info),
    summary: ensureObject(row.summary),
    peers: ensureObject(row.peers),
    tables: extractSectionTables(row),
    other_details: ensureObject(row.other_details),
    documents: ensureObject(row.documents),
  };

  SECTION_COLUMNS.forEach((col) => {
    normalized[col] = ensureObject(row[col]);
  });

  return normalized;
};

const createEntry = async (masterId, activeStockId, db = pool) => {
  await syncSerialSequence(db, "stock_screener_fundamentals", "id");
  const { rows } = await db.query(
    `
      INSERT INTO stock_screener_fundamentals (master_id, active_stock_id)
      VALUES ($1, $2)
      RETURNING *
    `,
    [Number(masterId), Number(activeStockId)],
  );
  return rows[0] ? normalizeFundamental(rows[0]) : null;
};

const linkActiveStockId = async (masterId, activeStockId, db = pool) => {
  const { rows } = await db.query(
    `
      UPDATE stock_screener_fundamentals
      SET active_stock_id = $2,
          updated_at = NOW()
      WHERE master_id = $1
      RETURNING *
    `,
    [Number(masterId), Number(activeStockId)],
  );

  return rows[0] ? normalizeFundamental(rows[0]) : null;
};

const upsertByMasterId = async (data, db = pool) => {
  const inputTables = ensureObject(data.tables);
  const quartersTable = ensureObject(data.quarters_table || inputTables.quarters);
  const profitLossTable = ensureObject(data.profit_loss_table || inputTables.profit_loss);
  const balanceSheetTable = ensureObject(data.balance_sheet_table || inputTables.balance_sheet);
  const cashFlowTable = ensureObject(data.cash_flow_table || inputTables.cash_flow);
  const ratiosTable = ensureObject(data.ratios_table || inputTables.ratios);
  const shareholdingsTable = ensureObject(data.shareholdings_table || inputTables.shareholdings);

  await syncSerialSequence(db, "stock_screener_fundamentals", "id");
  const { rows } = await db.query(
    `
      INSERT INTO stock_screener_fundamentals (
        master_id, active_stock_id, company, company_info, summary, peers,
        quarters_table, profit_loss_table, balance_sheet_table,
        cash_flow_table, ratios_table, shareholdings_table,
        other_details, documents,
        last_updated_at, updated_at
      )
      VALUES (
        $1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,
        $7::jsonb,$8::jsonb,$9::jsonb,
        $10::jsonb,$11::jsonb,$12::jsonb,
        $13::jsonb,$14::jsonb,
        $15,NOW()
      )
      ON CONFLICT (master_id)
      DO UPDATE SET
        active_stock_id = EXCLUDED.active_stock_id,
        company = EXCLUDED.company,
        company_info = EXCLUDED.company_info,
        summary = EXCLUDED.summary,
        peers = EXCLUDED.peers,
        quarters_table = EXCLUDED.quarters_table,
        profit_loss_table = EXCLUDED.profit_loss_table,
        balance_sheet_table = EXCLUDED.balance_sheet_table,
        cash_flow_table = EXCLUDED.cash_flow_table,
        ratios_table = EXCLUDED.ratios_table,
        shareholdings_table = EXCLUDED.shareholdings_table,
        other_details = EXCLUDED.other_details,
        documents = EXCLUDED.documents,
        last_updated_at = EXCLUDED.last_updated_at,
        updated_at = NOW()
      RETURNING *
    `,
    [
      Number(data.master_id),
      Number(data.active_stock_id),
      data.company || null,
      JSON.stringify(ensureObject(data.company_info)),
      JSON.stringify(ensureObject(data.summary)),
      JSON.stringify(ensureObject(data.peers)),
      JSON.stringify(quartersTable),
      JSON.stringify(profitLossTable),
      JSON.stringify(balanceSheetTable),
      JSON.stringify(cashFlowTable),
      JSON.stringify(ratiosTable),
      JSON.stringify(shareholdingsTable),
      JSON.stringify(ensureObject(data.other_details)),
      JSON.stringify(ensureObject(data.documents)),
      data.last_updated_at || new Date(),
    ],
  );
  return rows[0] ? normalizeFundamental(rows[0]) : null;
};

const updateLegacyEntry = async (data, db = pool) => {
  const { rows } = await db.query(
    `
      UPDATE stock_screener_fundamentals
      SET
        company = COALESCE($2, company),
        summary = COALESCE($3::jsonb, summary),
        last_updated_at = NOW(),
        updated_at = NOW()
      WHERE master_id = $1
      RETURNING *
    `,
    [
      Number(data.master_id),
      data.company || null,
      data.summary ? JSON.stringify(data.summary) : null,
    ],
  );
  return rows[0] ? normalizeFundamental(rows[0]) : null;
};

const getByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_screener_fundamentals WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] ? normalizeFundamental(rows[0]) : null;
};

const listMasterFreshness = async (db = pool) => {
  const { rows } = await db.query(
    `SELECT master_id, active_stock_id, last_updated_at FROM stock_screener_fundamentals`,
  );
  return rows;
};

module.exports = {
  normalizeFundamental,
  createEntry,
  linkActiveStockId,
  upsertByMasterId,
  updateLegacyEntry,
  getByMasterId,
  listMasterFreshness,
};
