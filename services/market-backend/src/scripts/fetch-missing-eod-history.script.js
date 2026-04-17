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

const getIndiaNowParts = () => {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(new Date());
  const values = parts.reduce((acc, part) => {
    if (part.type !== "literal") acc[part.type] = part.value;
    return acc;
  }, {});

  return {
    dateIso: `${values.year}-${values.month}-${values.day}`,
    hour: Number(values.hour || 0),
    minute: Number(values.minute || 0),
    second: Number(values.second || 0),
  };
};

const addDaysToIso = (isoDate, days) => {
  const date = new Date(`${isoDate}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
};

const INDIA_NOW = getIndiaNowParts();
const TODAY_ISO = INDIA_NOW.dateIso;
const RUNTIME_TODATE = HAS_TODAY_FLAG ? TODAY_ISO : process.env.EOD_HISTORY_TO_DATE || TODAY_ISO;
const TO_DATE =
  RUNTIME_TODATE === TODAY_ISO &&
  (INDIA_NOW.hour < 15 || (INDIA_NOW.hour === 15 && INDIA_NOW.minute < 30))
    ? addDaysToIso(TODAY_ISO, -1)
    : RUNTIME_TODATE;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const safeIsoDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
};

function getMissingHistoryStocks(rows = []) {
  const dedup = new Map();
  for (const row of rows) {
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

  if (String(master.eod_history_status || "").toUpperCase() === "NO_EOD_DATA") {
    return {
      skipped: true,
      statusUpdatedTo: "NO_EOD_DATA",
      count: 0,
      chunks: 0,
      latestStoredDate: null,
      message: "Skipped because stock is marked as NO_EOD_DATA",
    };
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
      const isoDate = safeIsoDate(candle?.date);
      if (!isoDate) continue;
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

  const hasAnyEodData =
    Number(result?.count || 0) > 0 || Boolean(result?.latestStoredDate) || Boolean(actualFromDate) || Boolean(actualToDate);
  const nextStatus = hasAnyEodData ? "HAS_EOD_DATA" : "NO_EOD_DATA";
  await masterService.updateMasterStock(masterId, { eod_history_status: nextStatus });

  return {
    skipped: false,
    statusUpdatedTo: nextStatus,
    count: Number(result?.count || 0),
    chunks: Array.isArray(result?.chunks) ? result.chunks.length : 0,
    latestStoredDate: result?.latestStoredDate || null,
    message: result?.message || "",
  };
}

async function run() {
  console.log(
    `Starting missing EOD history fetch (direct service mode). fromDate=${FROM_DATE}, toDate=${TO_DATE}, delayMs=${DELAY_MS}, useToday=${HAS_TODAY_FLAG}, marketGuardApplied=${RUNTIME_TODATE !== TO_DATE}, indiaNow=${TODAY_ISO} ${String(INDIA_NOW.hour).padStart(2, "0")}:${String(INDIA_NOW.minute).padStart(2, "0")}:${String(INDIA_NOW.second).padStart(2, "0")}`,
  );

  const masterStocks = await masterService.getAllMasterStocks();
  console.log(`Total master stocks fetched: ${masterStocks.length}`);

  const eligibleStocks = masterStocks.filter((row) => {
    return (
      Boolean(row?.is_active) &&
      String(row?.angelone_fetch_status || "").toLowerCase() === "fetched" &&
      String(row?.screener_status || "").toUpperCase() === "VALID" &&
      String(row?.eod_history_status || "").toUpperCase() !== "NO_EOD_DATA"
    );
  });
  console.log(
    `Eligible stocks (master active + angelone fetched + VALID + not NO_EOD_DATA): ${eligibleStocks.length}`,
  );

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
  console.log(`Eligible stocks selected for EOD sync check: ${targets.length}`);

  if (!targets.length) {
    console.log("No eligible stocks found for EOD sync.");
    return;
  }

  let success = 0;
  let failed = 0;
  let skipped = 0;

  for (let i = 0; i < targets.length; i += 1) {
    const row = targets[i];
    const masterId = Number(row.master_id);
    const label = `${row.name || row.symbol || "unknown"} (master_id=${masterId})`;

    try {
      console.log(
        `\n[${i + 1}/${targets.length}] Fetching EOD history for ${label} | from=${FROM_DATE} | to=${TO_DATE}`,
      );
      const result = await fetchHistoryForStock(masterId);
      if (result.skipped) {
        skipped += 1;
      } else {
        success += 1;
      }
      console.log(
        `${result.skipped ? "Skipped" : "Done"} ${label}: from=${FROM_DATE}, to=${TO_DATE}, count=${result.count}, chunks=${result.chunks}, latestStoredDate=${result.latestStoredDate || "-"}, eodHistoryStatus=${result.statusUpdatedTo || "-"}`,
      );
    } catch (error) {
      failed += 1;
      const msg =
        error?.response?.data?.message ||
        error?.response?.data?.error?.message ||
        error?.message;
      console.error(`Failed ${label}: ${msg}`);
      if (error?.stack) {
        console.error(error.stack);
      }
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
    skipped,
    failed,
    fromDate: FROM_DATE,
    toDate: TO_DATE,
  });
}

run().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
