require("dotenv").config();
require("../config/db");

const { pool } = require("../config/db");
const { withTransaction } = require("../repositories/tx");
const rawstocksRepo = require("../repositories/rawstocks.repository");
const stockMasterRepo = require("../repositories/stockMaster.repository");
const activeStocksRepo = require("../repositories/activeStocks.repository");
const stockMasterService = require("../services/stockMaster.service");
const activeStockService = require("../services/activestock.service");
const stockFundamentalsService = require("../services/stockFundamental.service");

const BATCH_SIZE = Math.max(1, Number(process.env.RAWSTOCKS_TO_MASTER_BATCH_SIZE || 500));

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
  names.some(
    (name) => argv.includes(`--${name}`) || argv.includes(`--${name}=true`),
  );

const runtime = {
  rawStockId: readArg("rawstock-id", "id"),
  token: readArg("token"),
  symbol: readArg("symbol"),
  once: hasFlag("once", "single", "test"),
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const ELIGIBLE_RAWSTOCKS_SQL = `
  SELECT
    id, token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status, security_code
  FROM public.rawstocks
  WHERE
    status = 'pending'
    AND (
      (exch_seg = 'NSE' AND instrumenttype = 'EQ'
        AND (symbol LIKE '%-EQ' OR symbol LIKE '%-BE' OR symbol LIKE '%-SM'))
      OR
      (exch_seg = 'BSE' AND instrumenttype = 'EQ'
        AND symbol NOT LIKE '%-RE'
        AND symbol NOT LIKE '%-RE1'
        AND symbol NOT LIKE '%-B'
        AND name NOT IN (
          SELECT name FROM public.rawstocks
          WHERE exch_seg = 'NSE' AND instrumenttype = 'EQ'
          AND (symbol LIKE '%-EQ' OR symbol LIKE '%-BE' OR symbol LIKE '%-SM')
        ))
    )
  ORDER BY symbol ASC
`;

const buildScreenerUrl = (rawStock) =>
  `https://www.screener.in/company/${encodeURIComponent(rawStock.name)}/consolidated/`;

const normalizeToken = (value) => String(value ?? "").trim();

const fetchEligibleRawStocks = async () => {
  if (runtime.rawStockId || runtime.token || runtime.symbol) {
    const clauses = [];
    const values = [];
    if (runtime.rawStockId) {
      values.push(Number(runtime.rawStockId));
      clauses.push(`id = $${values.length}`);
    } else if (runtime.token) {
      values.push(normalizeToken(runtime.token));
      clauses.push(`token = $${values.length}`);
    } else if (runtime.symbol) {
      values.push(String(runtime.symbol).trim());
      clauses.push(`symbol = $${values.length}`);
    }

    const { rows } = await pool.query(
      `
        SELECT
          id, token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status, security_code
        FROM public.rawstocks
        WHERE ${clauses.join(" AND ")}
        LIMIT 1
      `,
      values,
    );
    return rows;
  }

  const { rows } = await pool.query(ELIGIBLE_RAWSTOCKS_SQL);
  return rows;
};

const ensureMasterGraph = async (rawStock, client) => {
  const body = {
    token: rawStock.token,
    symbol: rawStock.symbol,
    name: rawStock.name,
    exchange: rawStock.exch_seg || rawStock.exchange,
    instrumenttype: rawStock.instrumenttype,
    lotsize: rawStock.lotsize,
    tick_size: rawStock.tick_size,
    raw_stock_id: rawStock.id,
    screener_status: "PENDING",
    screener_url: buildScreenerUrl(rawStock),
    security_code: rawStock.security_code,
  };

  let masterStock = await stockMasterRepo.getByToken(body.token, client);
  let created = false;

  if (!masterStock) {
    masterStock = await stockMasterService.createMasterStock(body, client);
    created = true;
  }

  let activeStock = await activeStocksRepo.getByMasterId(masterStock.id, client);
  if (!activeStock) {
    activeStock = await activeStockService.addStock(
      {
        ...body,
        master_id: masterStock.id,
      },
      client,
    );
  }

  const fundamentals = await stockFundamentalsService.getFullStockFundamentals(
    masterStock.id,
  );
  if (!fundamentals) {
    await stockFundamentalsService.createEntry(masterStock.id, activeStock.id, client);
  } else if (Number(fundamentals.active_stock_id) !== Number(activeStock.id)) {
    await stockFundamentalsService.linkActiveStockId(masterStock.id, activeStock.id, client);
  }

  await rawstocksRepo.updateById(rawStock.id, { status: "approved" }, client);

  return {
    created,
    masterStock,
    activeStock,
  };
};

const processOne = async (rawStock, index, total) => {
  const label = `${rawStock.symbol} | ${rawStock.name}`;
  console.log(`[rawstocks->master] ${index}/${total} start: ${label}`);

  try {
    const result = await withTransaction(async (client) => ensureMasterGraph(rawStock, client));
    console.log(
      `[rawstocks->master] ${index}/${total} done: ${label} | ${result.created ? "created" : "exists"}`,
    );
    return { ok: true, ...result };
  } catch (err) {
    console.error(
      `[rawstocks->master] ${index}/${total} failed: ${label} | error=${err?.message || err}`,
    );
    return { ok: false, error: err };
  }
};

const run = async () => {
  console.log("Starting rawstocks -> master worker");
  console.log(`Batch size hint=${BATCH_SIZE}`);

  const rows = await fetchEligibleRawStocks();
  if (!rows.length) {
    console.log("No eligible rawstocks found for master import.");
    return;
  }

  const total = runtime.once ? Math.min(1, rows.length) : rows.length;
  console.log(`Eligible rawstocks found: ${rows.length}`);

  const targetRows = runtime.once ? rows.slice(0, 1) : rows;
  let created = 0;
  let skipped = 0;
  let failed = 0;

  for (let i = 0; i < targetRows.length; i += 1) {
    const result = await processOne(targetRows[i], i + 1, total);
    if (result.ok) {
      if (result.created) created += 1;
      else skipped += 1;
    } else {
      failed += 1;
    }

    if (!runtime.once && i < targetRows.length - 1) {
      await sleep(50);
    }
  }

  console.log(
    `[rawstocks->master] complete | created=${created} skipped=${skipped} failed=${failed}`,
  );
};

run().catch((err) => {
  console.error("rawstocks -> master worker crashed", err);
  process.exit(1);
});
