require("dotenv").config();
process.env.PG_AUTO_MIGRATE = "false";
const { Pool } = require("pg");
const eodRepository = require("../repositories/eod.repository");
const {
  computeDerivedMetricsForCandles,
  toTradeDateKey,
} = require("../services/eodDerivedMetrics.service");
const stockMasterRepository = require("../repositories/stockMaster.repository");

const DATABASE_URL = process.env.PG_DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error("PG_DATABASE_URL is required");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PG_SSL === "true" ? { rejectUnauthorized: false } : false,
});

const dbReady = pool.query("SELECT 1");

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

const toPositiveNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? num : null;
};

const parseEodId = (value) => {
  if (!value) return { masterId: null, tradeDate: null };
  const [masterIdRaw, tradeDateRaw] = String(value).split(":");
  const masterId = Number(masterIdRaw);
  const tradeDate = String(tradeDateRaw || "").slice(0, 10);
  if (!Number.isFinite(masterId) || !tradeDate) {
    throw new Error("Invalid --eod-id format. Use --eod-id=<master_id>:<YYYY-MM-DD>");
  }
  return { masterId, tradeDate };
};

const eodId = parseEodId(readArg("eod-id", "row-id"));

const runtime = {
  masterId: toPositiveNumber(readArg("master-id", "stock-id", "id")) || eodId.masterId,
  tradeDate: readArg("trade-date", "date") || eodId.tradeDate || null,
  fromId: toPositiveNumber(readArg("from-id", "from-master-id")),
  toId: toPositiveNumber(readArg("to-id", "to-master-id")),
  batchSize: Math.max(1, Math.min(100, Number(readArg("batch-size") || process.env.EOD_DERIVED_BATCH_SIZE || 25))),
  updateChunkSize: Math.max(
    50,
    Math.min(5000, Number(readArg("update-chunk-size") || process.env.EOD_DERIVED_UPDATE_CHUNK || 1000)),
  ),
  shardCount: Math.max(1, Number(readArg("shards") || process.env.EOD_DERIVED_SHARDS || 1)),
  shardIndex: Math.max(0, Number(readArg("shard-index") || process.env.EOD_DERIVED_SHARD_INDEX || 0)),
  limitStocks: Math.max(0, Number(readArg("limit-stocks") || 0)),
  once: hasFlag("once", "single", "test"),
  dryRun: hasFlag("dry-run"),
};

if (runtime.shardIndex >= runtime.shardCount) {
  throw new Error("--shard-index must be less than --shards");
}

if (runtime.fromId && runtime.toId && runtime.fromId > runtime.toId) {
  throw new Error("--from-id must be less than or equal to --to-id");
}

const sliceUpdateRows = (rows = []) => {
  if (!runtime.tradeDate) return rows;
  const targetTradeDate = toTradeDateKey(runtime.tradeDate);
  return rows.filter((row) => toTradeDateKey(row.trade_date) === targetTradeDate);
};

const updateInChunks = async (rows = []) => {
  if (runtime.dryRun || !rows.length) return 0;

  let updated = 0;
  for (let index = 0; index < rows.length; index += runtime.updateChunkSize) {
    const chunk = rows.slice(index, index + runtime.updateChunkSize);
    updated += await eodRepository.bulkUpdateDerivedMetrics(chunk, pool);
  }
  return updated;
};

const processMasterId = async (masterId) => {
  const candles = await eodRepository.listAllCandlesByMasterIds([masterId], pool);
  if (!candles.length) {
    console.log(`[stock skip] master_id=${masterId} no_eod_rows`);
    return { processedStocks: 0, updatedRows: 0 };
  }

  const computed = computeDerivedMetricsForCandles(candles);
  const targetRows = sliceUpdateRows(computed);
  const updatedRows = await updateInChunks(targetRows);

  const firstDate = candles[0]?.trade_date ? toTradeDateKey(candles[0].trade_date) : "-";
  const lastDate = candles[candles.length - 1]?.trade_date
    ? toTradeDateKey(candles[candles.length - 1].trade_date)
    : "-";

  console.log(
    `[stock] master_id=${masterId} candles=${candles.length} range=${firstDate}..${lastDate} updated=${targetRows.length}${runtime.dryRun ? " dry-run" : ""}`,
  );

  return { processedStocks: 1, updatedRows };
};

const runSingleTarget = async () => {
  if (!runtime.masterId) {
    throw new Error("Single-target mode requires --master-id or --eod-id");
  }

  const result = await processMasterId(runtime.masterId);
  console.log("Single-target derived metrics completed", {
    masterId: runtime.masterId,
    tradeDate: runtime.tradeDate,
    dryRun: runtime.dryRun,
    ...result,
  });
};

const runRangeMode = async () => {
  const baseAfterMasterId = runtime.fromId ? runtime.fromId - 1 : 0;
  let afterMasterId = baseAfterMasterId;
  let totalProcessedStocks = 0;
  let totalUpdatedRows = 0;

  while (true) {
    if (runtime.limitStocks && totalProcessedStocks >= runtime.limitStocks) break;

    const remaining = runtime.limitStocks
      ? Math.max(runtime.limitStocks - totalProcessedStocks, 0)
      : runtime.batchSize;
    const batchLimit = runtime.limitStocks ? Math.min(runtime.batchSize, remaining) : runtime.batchSize;
    if (batchLimit <= 0) break;

    const masterIds = await stockMasterRepository.listEligibleMasterIdsForDerivedMetrics(
      {
        afterMasterId,
        fromId: runtime.fromId,
        toId: runtime.toId,
        limit: batchLimit,
        shardCount: runtime.shardCount,
        shardIndex: runtime.shardIndex,
      },
      pool,
    );

    if (!masterIds.length) break;

    for (const masterId of masterIds) {
      if (runtime.limitStocks && totalProcessedStocks >= runtime.limitStocks) break;
      const result = await processMasterId(masterId);
      totalProcessedStocks += result.processedStocks;
      totalUpdatedRows += result.updatedRows;
      afterMasterId = masterId;
    }

    console.log("Batch progress", {
      shard: `${runtime.shardIndex}/${runtime.shardCount}`,
      lastMasterId: afterMasterId,
      batchStocks: masterIds.length,
      totalProcessedStocks,
      totalUpdatedRows,
      dryRun: runtime.dryRun,
    });

    if (runtime.once) break;
  }

  console.log("Derived metrics worker completed", {
    shard: `${runtime.shardIndex}/${runtime.shardCount}`,
    fromId: runtime.fromId || null,
    toId: runtime.toId || null,
    limitStocks: runtime.limitStocks || null,
    batchSize: runtime.batchSize,
    updateChunkSize: runtime.updateChunkSize,
    totalProcessedStocks,
    totalUpdatedRows,
    dryRun: runtime.dryRun,
  });
};

const run = async () => {
  await dbReady;

  console.log("Starting EOD derived metrics worker", {
    masterId: runtime.masterId,
    tradeDate: runtime.tradeDate,
    fromId: runtime.fromId || null,
    toId: runtime.toId || null,
    batchSize: runtime.batchSize,
    updateChunkSize: runtime.updateChunkSize,
    shardCount: runtime.shardCount,
    shardIndex: runtime.shardIndex,
    limitStocks: runtime.limitStocks || null,
    once: runtime.once,
    dryRun: runtime.dryRun,
  });

  if (runtime.masterId) {
    await runSingleTarget();
    return;
  }

  await runRangeMode();
};

run()
  .catch((error) => {
    console.error("Fatal EOD derived metrics worker error:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
