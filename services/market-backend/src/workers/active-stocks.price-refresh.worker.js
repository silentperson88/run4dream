require("dotenv").config();
require("../config/db");

const { pool } = require("../config/db");
const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const { SmartApiPriceService } = require("../services/smartapi.service");
const { withTransaction } = require("../repositories/tx");

const smartApiPriceService = new SmartApiPriceService();

const BATCH_SIZE = 50;
const PAUSE_MS = Number(process.env.ACTIVE_PRICE_REFRESH_PAUSE_MS || 10_000);
const DEFAULT_MODE = "FULL";

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
  mode: String(readArg("mode") || DEFAULT_MODE).toUpperCase(),
  once: hasFlag("once", "single", "test"),
  limit: Number(readArg("limit") || 0) || 0,
  exchange: (readArg("exchange") || "").toUpperCase(),
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeToken = (value) => String(value ?? "").trim();
const pickNumber = (...values) => {
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return 0;
};

const setAngelOneStatus = async (masterId, status) => {
  if (!masterId) return null;
  return stockMasterService.updateMasterStock(Number(masterId), {
    angelone_fetch_status: status,
  });
};

const chunk = (items, size) => {
  const batches = [];
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size));
  }
  return batches;
};

const fetchActiveStocks = async () => {
  const values = [];
  const where = ["sm.is_active = TRUE"];

  if (runtime.exchange === "NSE" || runtime.exchange === "BSE") {
    values.push(runtime.exchange);
    where.push(`upper(exchange) = $${values.length}`);
  }

  const whereClause = `WHERE ${where.join(" AND ")}`;
  const sql = `
    SELECT
      a.id,
      a.master_id,
      a.token,
      a.symbol,
      a.name,
      a.exchange,
      sm.is_active AS master_is_active
    FROM active_stock a
    INNER JOIN stock_master sm ON sm.id = a.master_id
    ${whereClause}
    ORDER BY
      CASE
        WHEN upper(a.exchange) = 'NSE' THEN 0
        WHEN upper(a.exchange) = 'BSE' THEN 1
        ELSE 2
      END,
      a.added_at ASC
  `;

  const { rows } = await pool.query(sql, values);
  return rows;
};

const splitIntoExchangeBatches = (stocks) => {
  const nseStocks = [];
  const bseStocks = [];

  for (const row of stocks) {
    const exchange = String(row.exchange || "NSE").toUpperCase();
    if (exchange === "BSE") {
      bseStocks.push(row);
    } else {
      nseStocks.push(row);
    }
  }

  return [...chunk(nseStocks, BATCH_SIZE), ...chunk(bseStocks, BATCH_SIZE)];
};

const getPriceForBatch = async (batch) => {
  const byExchange = new Map();
  for (const row of batch) {
    const exchange = String(row.exchange || "NSE").toUpperCase();
    if (!byExchange.has(exchange)) byExchange.set(exchange, []);
    byExchange.get(exchange).push(row);
  }

  const fetchedByToken = new Map();
  const unfetchedTokenSet = new Set();

  for (const [exchange, rows] of byExchange.entries()) {
    const tokenIds = rows.map((r) => normalizeToken(r.token)).filter(Boolean);
    if (!tokenIds.length) continue;

    const res = await smartApiPriceService.getMarketData(runtime.mode, tokenIds, exchange);
    const payload = res?.data || {};
    const fetched = Array.isArray(payload?.fetched) ? payload.fetched : [];
    const rawUnfetched = Array.isArray(payload?.unfetched) ? payload.unfetched : [];
    const fetchedTokenSet = new Set();

    for (const item of fetched) {
      const token = normalizeToken(
        item?.symbolToken ?? item?.symboltoken ?? item?.token,
      );
      if (!token) continue;
      fetchedTokenSet.add(token);
      fetchedByToken.set(token, item);
    }

    for (const item of rawUnfetched) {
      const token = normalizeToken(
        item?.symbolToken ?? item?.symboltoken ?? item?.token,
      );
      if (token) unfetchedTokenSet.add(token);
    }

    for (const token of tokenIds) {
      if (!fetchedTokenSet.has(token) && !unfetchedTokenSet.has(token)) {
        unfetchedTokenSet.add(token);
      }
    }
  }

  return { fetchedByToken, unfetchedTokenSet };
};

const applyPriceBatch = async (batch, priceResult) => {
  const updated = [];
  const failed = [];
  const skipped = [];
  const unfetched = [];

  for (const row of batch) {
    const token = normalizeToken(row.token);
    if (!token) {
      skipped.push(row);
      await setAngelOneStatus(row.master_id, "skipped_tokenless");
      continue;
    }
    if (priceResult.unfetchedTokenSet.has(token)) {
      unfetched.push(row);
      await setAngelOneStatus(row.master_id, "unfetched");
      continue;
    }

    const fetched = priceResult.fetchedByToken.get(token);

    const ltp = pickNumber(fetched?.ltp, fetched?.close, fetched?.open, fetched?.high);
    if (!(ltp > 0)) {
      failed.push(row);
      await setAngelOneStatus(row.master_id, "failed");
      continue;
    }

    const updatedRow = await activeStockService.updateActiveStockPrice(token, {
      ltp,
      open: pickNumber(fetched?.open),
      high: pickNumber(fetched?.high),
      low: pickNumber(fetched?.low),
      close: pickNumber(fetched?.close),
      percentChange: pickNumber(fetched?.percentChange, fetched?.change, fetched?.netChange),
      avgPrice: pickNumber(fetched?.avgPrice, fetched?.averagePrice),
      lowerCircuit: pickNumber(fetched?.lowerCircuit),
      upperCircuit: pickNumber(fetched?.upperCircuit),
      week52Low: pickNumber(fetched?.week52Low, fetched?.["52WeekLow"]),
      week52High: pickNumber(fetched?.week52High, fetched?.["52WeekHigh"]),
    });

    if (!updatedRow) {
      failed.push(row);
      await setAngelOneStatus(row.master_id, "failed");
      continue;
    }

    updated.push(row);
    await setAngelOneStatus(row.master_id, "fetched");
  }

  return { updated, failed, skipped, unfetched };
};

const deleteLinkedMasterGraph = async (masterId) => {
  if (!masterId) return false;
  await withTransaction(async (client) => {
    await client.query(`DELETE FROM stock_master WHERE id = $1`, [Number(masterId)]);
  });
  return true;
};

const run = async () => {
  console.log("Starting active stock price refresh worker");
  console.log(`Mode=${runtime.mode}, batchSize=${BATCH_SIZE}, pause=${PAUSE_MS}ms`);

  const stocks = await fetchActiveStocks();
  if (!stocks.length) {
    console.log("No active stocks found.");
    return;
  }

  const selectedStocks = runtime.limit > 0 ? stocks.slice(0, runtime.limit) : stocks;
  const batches = splitIntoExchangeBatches(selectedStocks);
  const exchangeCounts = selectedStocks.reduce(
    (acc, row) => {
      const exchange = String(row.exchange || "NSE").toUpperCase();
      acc[exchange] = (acc[exchange] || 0) + 1;
      return acc;
    },
    { NSE: 0, BSE: 0 },
  );

  console.log(
    `Selected ${selectedStocks.length} active stocks from ${stocks.length} total ` +
      `(limit=${runtime.limit > 0 ? runtime.limit : "none"}; NSE=${exchangeCounts.NSE || 0}, BSE=${exchangeCounts.BSE || 0})`,
  );
  console.log(`Created ${batches.length} exchange batch(es)`);

  let totalUpdated = 0;
  let totalFailed = 0;
  let totalDeleted = 0;
  let totalSkipped = 0;
  let totalUnfetched = 0;

  for (let i = 0; i < batches.length; i += 1) {
    const batch = batches[i];
    console.log(`Batch ${i + 1}/${batches.length}: ${batch.length} active stocks`);

    let priceResult;
    try {
      priceResult = await getPriceForBatch(batch);
    } catch (err) {
      console.error(
        `Batch ${i + 1} price fetch failed:`,
        err?.response?.data || err?.message || err,
      );
      totalFailed += batch.length;
      if (i < batches.length - 1) {
        await sleep(PAUSE_MS);
      }
      continue;
    }

    const { updated, failed, skipped, unfetched } = await applyPriceBatch(batch, priceResult);
    totalUpdated += updated.length;
    totalFailed += failed.length;
    totalSkipped += skipped.length;
    totalUnfetched += unfetched.length;

    for (const row of failed) {
      try {
        await deleteLinkedMasterGraph(row.master_id);
        totalDeleted += 1;
        console.log(
          `[active-price] Deleted failed stock from master graph: ${row.symbol} (${row.token})`,
        );
      } catch (err) {
        console.error(
          `[active-price] Failed to delete master graph for ${row.symbol} (${row.token}):`,
          err?.message || err,
        );
      }
    }

    console.log(
      `Batch ${i + 1} done | updated=${updated.length} unfetched=${unfetched.length} failed=${failed.length} skipped=${skipped.length} deleted=${failed.length}`,
    );

    if (i < batches.length - 1) {
      await sleep(PAUSE_MS);
    }
  }

  console.log("\nCompleted active stock price refresh.");
  console.log({
    totalProcessed: selectedStocks.length,
    totalUpdated,
    totalFailed,
    totalSkipped,
    totalUnfetched,
    totalDeleted,
  });
};

run().catch((err) => {
  console.error("Active stock price refresh worker crashed", err);
  process.exit(1);
});
