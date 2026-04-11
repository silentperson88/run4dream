require("dotenv").config();
require("../config/db");

const { pool, dbReady } = require("../config/db");
const stockFundamentalsService = require("../services/stockFundamental.service");
const stockMasterService = require("../services/stockMaster.service");
const activeStockService = require("../services/activestock.service");

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

const toNumberOrNull = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildOverviewRow = (snapshot, master) => {
  const companyInfo = snapshot?.company_info || {};
  const summary = snapshot?.summary || {};
  const marketSnapshot = summary?.market_snapshot || {};

  return {
    company_name: snapshot?.company || companyInfo?.company_name || master?.name || null,
    about: companyInfo?.about || null,
    key_points: companyInfo?.key_points || null,
    market_cap: toNumberOrNull(marketSnapshot?.market_cap),
    current_price: toNumberOrNull(marketSnapshot?.current_price),
    high_low: marketSnapshot?.high_low || null,
    stock_pe: toNumberOrNull(marketSnapshot?.stock_pe ?? marketSnapshot?.pe_ratio),
    book_value: toNumberOrNull(marketSnapshot?.book_value),
    dividend_yield: toNumberOrNull(marketSnapshot?.dividend_yield),
    roce: toNumberOrNull(marketSnapshot?.roce),
    roe: toNumberOrNull(marketSnapshot?.roe),
    face_value: toNumberOrNull(marketSnapshot?.face_value),
    pros: Array.isArray(summary?.pros) ? summary.pros : [],
    cons: Array.isArray(summary?.cons) ? summary.cons : [],
    links: Array.isArray(companyInfo?.links) ? companyInfo.links : [],
    source_payload: snapshot || {},
  };
};

const resolveActiveStockId = async (snapshot, master) => {
  const direct = Number(snapshot?.active_stock_id || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;

  const existing = await activeStockService.getActiveStockByMasterId(master.id);
  if (existing?.id) return existing.id;

  const created = await activeStockService.addStock({
    master_id: master.id,
    token: master.token,
    symbol: master.symbol,
    name: master.name,
    exchange: master.exchange,
    security_code: master.security_code,
    instrumenttype: master.instrumenttype || "EQ",
  });

  return created?.id || null;
};

const upsertOverview = async (snapshot, master) => {
  const activeStockId = await resolveActiveStockId(snapshot, master);
  if (!activeStockId) {
    throw new Error(`active_stock_id not found for master_id=${master.id}`);
  }

  const overview = buildOverviewRow(snapshot, master);
  const { rows } = await pool.query(
    `
      INSERT INTO stock_fundamental_overview (
        master_id,
        active_stock_id,
        snapshot_id,
        company_name,
        about,
        key_points,
        market_cap,
        current_price,
        high_low,
        stock_pe,
        book_value,
        dividend_yield,
        roce,
        roe,
        face_value,
        pros,
        cons,
        links,
        source_payload,
        last_updated_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,
        $7,$8,$9,$10,$11,$12,
        $13,$14,$15,$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,
        $20,$21
      )
      ON CONFLICT (master_id)
      DO UPDATE SET
        active_stock_id = EXCLUDED.active_stock_id,
        snapshot_id = EXCLUDED.snapshot_id,
        company_name = EXCLUDED.company_name,
        about = EXCLUDED.about,
        key_points = EXCLUDED.key_points,
        market_cap = EXCLUDED.market_cap,
        current_price = EXCLUDED.current_price,
        high_low = EXCLUDED.high_low,
        stock_pe = EXCLUDED.stock_pe,
        book_value = EXCLUDED.book_value,
        dividend_yield = EXCLUDED.dividend_yield,
        roce = EXCLUDED.roce,
        roe = EXCLUDED.roe,
        face_value = EXCLUDED.face_value,
        pros = EXCLUDED.pros,
        cons = EXCLUDED.cons,
        links = EXCLUDED.links,
        source_payload = EXCLUDED.source_payload,
        last_updated_at = EXCLUDED.last_updated_at,
        updated_at = EXCLUDED.updated_at
      RETURNING id
    `,
    [
      Number(master.id),
      Number(activeStockId),
      Number(snapshot?.id || 0) || null,
      overview.company_name,
      overview.about,
      overview.key_points,
      overview.market_cap,
      overview.current_price,
      overview.high_low,
      overview.stock_pe,
      overview.book_value,
      overview.dividend_yield,
      overview.roce,
      overview.roe,
      overview.face_value,
      JSON.stringify(overview.pros),
      JSON.stringify(overview.cons),
      JSON.stringify(overview.links),
      JSON.stringify(overview.source_payload),
      snapshot?.last_updated_at || snapshot?.updated_at || new Date(),
      new Date(),
    ],
  );

  return rows[0] || null;
};

const loadSnapshots = async () => {
  if (runtime.masterId) {
    const master = await stockMasterService.getMasterStockById(runtime.masterId);
    if (!master) return [];
    const snapshot = await stockFundamentalsService.getFullStockFundamentals(master.id);
    return snapshot ? [{ snapshot, master }] : [];
  }

  if (runtime.symbol) {
    const master = await stockMasterService.getMasterStockBySymbol(runtime.symbol);
    if (!master) return [];
    const snapshot = await stockFundamentalsService.getFullStockFundamentals(master.id);
    return snapshot ? [{ snapshot, master }] : [];
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
    snapshot,
    master: {
      id: snapshot.master_id,
      symbol: snapshot.symbol,
      name: snapshot.name,
      exchange: snapshot.exchange,
      token: snapshot.token,
      security_code: snapshot.security_code,
    },
  }));
};

const run = async () => {
  await dbReady;

  console.log(
    `Starting fundamentals overview worker. once=${runtime.once}, masterId=${runtime.masterId || "all"}, symbol=${runtime.symbol || "all"}`,
  );

  const items = await loadSnapshots();
  console.log(`Snapshots loaded: ${items.length}`);

  let success = 0;
  let failed = 0;

  for (let i = 0; i < items.length; i += 1) {
    const { snapshot, master } = items[i];
    const label = snapshot?.company || master?.name || `master_id=${master?.id}`;

    try {
      await upsertOverview(snapshot, master);
      success += 1;
      console.log(`[${i + 1}/${items.length}] OK ${label}`);
    } catch (error) {
      failed += 1;
      console.error(
        `[${i + 1}/${items.length}] Failed master_id=${master?.id}:`,
        error?.message || error,
      );
    }

    if (runtime.once) break;
  }

  console.log("\nOverview worker completed.");
  console.log({
    total: items.length,
    success,
    failed,
    once: runtime.once,
    masterId: runtime.masterId || null,
    symbol: runtime.symbol || null,
  });
};

run()
  .catch((error) => {
    console.error("Fatal overview worker error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
