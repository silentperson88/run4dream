require("dotenv").config();
const {
  fetchEodByMasterIdRangeChunked,
} = require("../services/stockOhlcEod.service");
const masterService = require("../services/stockMaster.service");
const { getAllActiveStocks } = require("../services/activestock.service");

const DELAY_MS = Number(process.env.EOD_HISTORY_DELAY_MS || 20_000);
const FROM_DATE = process.env.EOD_HISTORY_FROM_DATE || "2007-01-01";
const TO_DATE =
  process.env.EOD_HISTORY_TO_DATE || new Date().toISOString().slice(0, 10);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getMissingHistoryStocks(rows = []) {
  const out = rows.filter(
    (row) =>
      !row?.hasHistoryData ||
      !row?.historyDataFromDate ||
      !row?.historyDataToDate,
  );

  const dedup = new Map();
  for (const row of out) {
    const id = Number(row?.master_id);
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
    `Starting missing EOD history fetch (direct service mode). fromDate=${FROM_DATE}, toDate=${TO_DATE}, delayMs=${DELAY_MS}`,
  );

  const activeStocks = await getAllActiveStocks();
  console.log(`Total active stocks fetched: ${activeStocks.length}`);

  const targets = getMissingHistoryStocks(activeStocks);
  console.log(`Active stocks with missing history: ${targets.length}`);

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
