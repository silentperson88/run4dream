require("dotenv").config();
const {
  fetchEodByMasterIdRangeChunked,
} = require("../services/stockOhlcEod.service");
const masterService = require("../services/stockMaster.service");
const eodRepo = require("../repositories/eod.repository");

const DELAY_MS = Number(process.env.EOD_HISTORY_DELAY_MS || 20_000);
const CLI_ARGS = process.argv.slice(2);
const HAS_TODAY_FLAG = CLI_ARGS.includes("--today");
const FROM_DATE = process.env.EOD_HISTORY_FROM_DATE || "2007-01-01";
const TODAY_ISO = new Date().toISOString().slice(0, 10);
const TO_DATE = HAS_TODAY_FLAG
  ? TODAY_ISO
  : process.env.EOD_HISTORY_TO_DATE || TODAY_ISO;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getMissingHistoryStocks(rows = []) {
  const out = rows.filter((row) => {
    const latestDate = String(row?.latestTradeDate || "").trim();
    if (!latestDate) return true;
    return latestDate < TO_DATE;
  });

  const dedup = new Map();
  for (const row of out) {
    const id = Number(row?.master_id || row?.id);
    if (!Number.isFinite(id) || id <= 0) continue;
    if (!dedup.has(id)) dedup.set(id, row);
  }

  return Array.from(dedup.values());
}

async function fetchHistoryForStock(masterId) {
  const master = await masterService.getMasterStockById(masterId);
  if (!master) {
    throw new Error(`Invalid master_id: ${masterId}`);
  }

  if (
    !master.is_active ||
    String(master.angelone_fetch_status || "").toLowerCase() !== "fetched" ||
    String(master.screener_status || "").toUpperCase() !== "VALID"
  ) {
    throw new Error(
      `Stock is not eligible for EOD history fetch (requires active + angelone fetched + screener VALID)`,
    );
  }

  const result = await fetchEodByMasterIdRangeChunked({
    master_id: Number(masterId),
    symboltoken: master.token,
    symbol: master.symbol,
    fromDate: FROM_DATE,
    toDate: TO_DATE,
    exchange: master.exchange,
  });

  let actualFromDate = null;
  let actualToDate = null;
  if (Array.isArray(result?.data) && result.data.length) {
    for (const candle of result.data) {
      const isoDate = new Date(candle?.date).toISOString().slice(0, 10);
      if (!actualFromDate || isoDate < actualFromDate) actualFromDate = isoDate;
      if (!actualToDate || isoDate > actualToDate) actualToDate = isoDate;
    }
  }

  await masterService.updateHistoryCoverage(masterId, {
    requestedFromDate: FROM_DATE,
    requestedToDate: TO_DATE,
    actualFromDate,
    actualToDate,
  });

  return {
    count: Number(result?.count || 0),
    chunks: Array.isArray(result?.chunks) ? result.chunks.length : 0,
    latestStoredDate: result?.latestStoredDate || null,
    message: result?.message || "",
  };
}

async function run() {
  console.log(
    `Starting missing EOD history fetch (direct service mode). fromDate=${FROM_DATE}, toDate=${TO_DATE}, delayMs=${DELAY_MS}, useToday=${HAS_TODAY_FLAG}`,
  );

  const masterStocks = await masterService.getAllMasterStocks();
  console.log(`Total master stocks fetched: ${masterStocks.length}`);

  const eligibleStocks = masterStocks.filter((row) => {
    return (
      Boolean(row?.is_active) &&
      String(row?.angelone_fetch_status || "").toLowerCase() === "fetched" &&
      String(row?.screener_status || "").toUpperCase() === "VALID"
    );
  });
  console.log(`Eligible stocks (master active + angelone fetched + VALID): ${eligibleStocks.length}`);

  const latestByMasterId = await eodRepo.getLatestTradeDatesByMasterIds(
    eligibleStocks.map((row) => row.id),
  );

  const targets = getMissingHistoryStocks(
    eligibleStocks.map((row) => ({
      ...row,
      master_id: row.id,
      latestTradeDate: latestByMasterId.get(Number(row.id)) || null,
    })),
  );
  console.log(`Eligible stocks with missing EOD history: ${targets.length}`);

  if (!targets.length) {
    console.log("No stocks require history fetch.");
    return;
  }

  let success = 0;
  let failed = 0;

  for (let i = 0; i < targets.length; i += 1) {
    const row = targets[i];
    const masterId = Number(row.master_id);
    const label = `${row.name || row.symbol || "unknown"} (master_id=${masterId})`;

    try {
      console.log(
        `\n[${i + 1}/${targets.length}] Fetching EOD history for ${label}`,
      );
      const result = await fetchHistoryForStock(masterId);
      success += 1;
      console.log(
        `Done ${label}: count=${result.count}, chunks=${result.chunks}, latestStoredDate=${result.latestStoredDate || "-"}`,
      );
    } catch (error) {
      failed += 1;
      const msg =
        error?.response?.data?.message ||
        error?.response?.data?.error?.message ||
        error?.message;
      console.error(`Failed ${label}: ${msg}`);
    }

    if (i < targets.length - 1) {
      console.log(`Waiting ${DELAY_MS / 1000}s before next stock...`);
      await sleep(DELAY_MS);
    }
  }

  console.log("\nMissing EOD history job completed.");
  console.log({
    totalTargets: targets.length,
    success,
    failed,
    fromDate: FROM_DATE,
    toDate: TO_DATE,
  });
}

run().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
