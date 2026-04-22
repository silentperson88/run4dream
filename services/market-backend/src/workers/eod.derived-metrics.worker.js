require("dotenv").config();
require("../config/db");

const { pool, dbReady } = require("../config/db");
const eodRepository = require("../repositories/eod.repository");
const { computeDerivedMetricsForCandles, toTradeDateKey } = require("../services/eodDerivedMetrics.service");

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
  masterId: Number(readArg("master-id", "stock-id", "id") || eodId.masterId || 0) || null,
  tradeDate: readArg("trade-date", "date") || eodId.tradeDate || null,
  batchSize: Math.max(1, Math.min(100, Number(readArg("batch-size") || process.env.EOD_DERIVED_BATCH_SIZE || 25))),
  updateChunkSize: Math.max(
    50,
    Math.min(5000, Number(readArg("update-chunk-size") || process.env.EOD_DERIVED_UPDATE_CHUNK || 1000)),
  ),
  shardCount: Math.max(1, Number(readArg("shards") || process.env.EOD_DERIVED_SHARDS || 1)),
  shardIndex: Math.max(0, Number(readArg("shard-index") || process.env.EOD_DERIVED_SHARD_INDEX || 0)),
  fromMasterId: Math.max(0, Number(readArg("from-master-id") || 0)),
  limitStocks: Math.max(0, Number(readArg("limit-stocks") || 0)),
  once: hasFlag("once", "single", "test"),
  dryRun: hasFlag("dry-run"),
};

if (runtime.shardIndex >= runtime.shardCount) {
  throw new Error("--shard-index must be less than --shards");
}

const groupCandlesByMasterId = (rows = []) => {
  const grouped = new Map();
  for (const row of rows) {
    const key = Number(row.master_id);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }
  return grouped;
};

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

const processMasterIds = async (masterIds = []) => {
  if (!masterIds.length) {
    return {
      processedStocks: 0,
      updatedRows: 0,
    };
  }

  const rows = await eodRepository.listAllCandlesByMasterIds(masterIds, pool);
  const grouped = groupCandlesByMasterId(rows);

  let updatedRows = 0;
  let processedStocks = 0;

  for (const masterId of masterIds) {
    const candles = grouped.get(Number(masterId)) || [];
    if (!candles.length) continue;

    const computed = computeDerivedMetricsForCandles(candles);
    const targetRows = sliceUpdateRows(computed);
    updatedRows += await updateInChunks(targetRows);
    processedStocks += 1;

    const firstDate = candles[0]?.trade_date ? toTradeDateKey(candles[0].trade_date) : "-";
    const lastDate = candles[candles.length - 1]?.trade_date
      ? toTradeDateKey(candles[candles.length - 1].trade_date)
      : "-";
    console.log(
      `[stock ${processedStocks}/${masterIds.length}] master_id=${masterId} candles=${candles.length} range=${firstDate}..${lastDate} updated=${targetRows.length}${runtime.dryRun ? " dry-run" : ""}`,
    );
  }

  return { processedStocks, updatedRows };
};

const runSingleTarget = async () => {
  if (!runtime.masterId) {
    throw new Error("Single-target mode requires --master-id or --eod-id");
  }

  const result = await processMasterIds([runtime.masterId]);
  console.log("Single-target derived metrics completed", {
    masterId: runtime.masterId,
    tradeDate: runtime.tradeDate,
    dryRun: runtime.dryRun,
    ...result,
  });
};

const runBatchMode = async () => {
  let afterMasterId = runtime.fromMasterId;
  let totalProcessedStocks = 0;
  let totalUpdatedRows = 0;

  while (true) {
    if (runtime.limitStocks && totalProcessedStocks >= runtime.limitStocks) break;

    const remaining = runtime.limitStocks ? Math.max(runtime.limitStocks - totalProcessedStocks, 0) : runtime.batchSize;
    const limit = runtime.limitStocks ? Math.min(runtime.batchSize, remaining) : runtime.batchSize;
    if (limit <= 0) break;

    const masterIds = await eodRepository.listMasterIdsForDerivedMetrics(
      {
        afterMasterId,
        limit,
        shardCount: runtime.shardCount,
        shardIndex: runtime.shardIndex,
      },
      pool,
    );

    if (!masterIds.length) break;

    const result = await processMasterIds(masterIds);
    totalProcessedStocks += result.processedStocks;
    totalUpdatedRows += result.updatedRows;
    afterMasterId = masterIds[masterIds.length - 1];

    console.log("Batch progress", {
      shard: `${runtime.shardIndex}/${runtime.shardCount}`,
      lastMasterId: afterMasterId,
      batchStocks: result.processedStocks,
      batchUpdatedRows: result.updatedRows,
      totalProcessedStocks,
      totalUpdatedRows,
      dryRun: runtime.dryRun,
    });

    if (runtime.once) break;
  }

  console.log("Derived metrics worker completed", {
    shard: `${runtime.shardIndex}/${runtime.shardCount}`,
    fromMasterId: runtime.fromMasterId,
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
    batchSize: runtime.batchSize,
    updateChunkSize: runtime.updateChunkSize,
    shardCount: runtime.shardCount,
    shardIndex: runtime.shardIndex,
    fromMasterId: runtime.fromMasterId,
    limitStocks: runtime.limitStocks || null,
    once: runtime.once,
    dryRun: runtime.dryRun,
  });

  if (runtime.masterId) {
    await runSingleTarget();
    return;
  }

  await runBatchMode();
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
