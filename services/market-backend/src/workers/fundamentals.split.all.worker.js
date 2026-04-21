require("dotenv").config();
require("../config/db");

const { pool, dbReady } = require("../config/db");
const stockFundamentalsService = require("../services/stockFundamental.service");
const stockMasterService = require("../services/stockMaster.service");
const activeStockService = require("../services/activestock.service");
const {
  SPLIT_TABLES,
  ensureActiveStockId,
  upsertSectionRows,
} = require("../repositories/fundamentalsSplit.repository");

const argv = process.argv.slice(2);
const readArg = (...names) => {
  for (const name of names) {
    const prefix = `--${name}=`;
    const hit = argv.find((arg) => arg.startsWith(prefix));
    if (hit) return hit.slice(prefix.length);
  }
  return null;
};
const hasFlag = (...names) =>
  names.some((name) => argv.includes(`--${name}`) || argv.includes(`--${name}=true`));

const runtime = {
  masterId: readArg("master-id", "stock-id", "id"),
  symbol: readArg("symbol"),
  once: hasFlag("once", "single", "test"),
};

const SECTION_KEYS = Object.keys(SPLIT_TABLES);

const loadCandidates = async () => {
  if (runtime.masterId) {
    const master = await stockMasterService.getMasterStockById(runtime.masterId);
    if (!master) return [];
    const snapshot = await stockFundamentalsService.getFullStockFundamentals(master.id);
    return snapshot ? [{ master, snapshot }] : [];
  }

  if (runtime.symbol) {
    const master = await stockMasterService.getMasterStockBySymbol(runtime.symbol);
    if (!master) return [];
    const snapshot = await stockFundamentalsService.getFullStockFundamentals(master.id);
    return snapshot ? [{ master, snapshot }] : [];
  }

  const { rows } = await pool.query(
    `
      SELECT sf.*, sm.symbol, sm.name, sm.exchange, sm.token, sm.security_code
      FROM stock_screener_fundamentals sf
      INNER JOIN stock_master sm ON sm.id = sf.master_id
      WHERE sm.is_active = TRUE
      ORDER BY sf.master_id ASC, sf.id ASC
    `,
  );

  return rows.map((snapshot) => ({
    master: {
      id: snapshot.master_id,
      symbol: snapshot.symbol,
      name: snapshot.name,
      exchange: snapshot.exchange,
      token: snapshot.token,
      security_code: snapshot.security_code,
    },
    snapshot,
  }));
};

const processCandidate = async (master, snapshot) => {
  const label = master?.name || master?.symbol || `master_id=${master?.id || "unknown"}`;
  const activeStockId = await ensureActiveStockId(master, activeStockService, snapshot);

  if (!activeStockId) {
    throw new Error(`active_stock_id not found for master_id=${master?.id}`);
  }

  const sectionResults = [];
  let totalRows = 0;

  for (const sectionKey of SECTION_KEYS) {
    const result = await upsertSectionRows(sectionKey, master, snapshot, activeStockId, pool);
    const count = result?.count || 0;
    totalRows += count;
    sectionResults.push({
      section: sectionKey,
      rows: count,
    });
  }

  return {
    label,
    totalRows,
    sectionResults,
  };
};

const run = async () => {
  await dbReady;

  console.log(
    `Starting fundamentals split-all worker. once=${runtime.once}, masterId=${runtime.masterId || "all"}, symbol=${runtime.symbol || "all"}`,
  );

  const items = await loadCandidates();
  console.log(`split-all candidates loaded: ${items.length}`);

  let success = 0;
  let failed = 0;

  for (let i = 0; i < items.length; i += 1) {
    const { master, snapshot } = items[i];
    try {
      const { label, totalRows, sectionResults } = await processCandidate(master, snapshot);
      success += 1;
      console.log(
        `[${i + 1}/${items.length}] OK ${label} | totalRows=${totalRows} | ${sectionResults
          .map((item) => `${item.section}=${item.rows}`)
          .join(", ")}`,
      );
    } catch (error) {
      failed += 1;
      console.error(`[${i + 1}/${items.length}] Failed master_id=${master?.id}:`, error?.message || error);
    }

    if (runtime.once) break;
  }

  console.log(`\nsplit-all worker completed.`);
  console.log({
    total: items.length,
    success,
    failed,
    once: runtime.once,
    masterId: runtime.masterId || null,
    symbol: runtime.symbol || null,
    sections: SECTION_KEYS,
  });
};

run()
  .catch((error) => {
    console.error("Fatal fundamentals split-all worker error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
