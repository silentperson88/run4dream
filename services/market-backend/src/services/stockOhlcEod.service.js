const eodRepo = require("../repositories/eod.repository");
const { SmartApiPriceService } = require("./smartapi.service");
const { toSmartApiDate, normalizeEodDate } = require("../utils/Mthods.utils");

const smartApiPriceService = new SmartApiPriceService();

async function createOhlcEod(payload) {
  return createEodForAllStocks([payload]);
}

async function createEodForAllStocks(eodData = []) {
  if (!eodData.length) return;

  let inserted = 0;
  for (const doc of eodData) {
    await eodRepo.upsertDailyCandle(doc);
    inserted += 1;
  }

  return {
    insertedCount: inserted,
    writeErrors: 0,
  };
}

function safeNormalizeEodDate(timestamp) {
  if (!timestamp) return null;
  try {
    const date = normalizeEodDate(timestamp);
    if (!date || Number.isNaN(date.getTime())) return null;
    return date;
  } catch (error) {
    return null;
  }
}

async function fetchEodByMasterIdRange({
  master_id,
  symboltoken,
  fromDate,
  toDate,
  exchange = "NSE",
  symbol,
}) {
  if (!master_id || !symboltoken || !fromDate || !toDate || !symbol) {
    throw new Error("master_id, symboltoken, fromDate, toDate are required");
  }

  const from = new Date(`${fromDate}T00:00:00.000Z`);
  const to = new Date(`${toDate}T00:00:00.000Z`);
  if (from > to) {
    throw new Error("fromDate must be less than or equal to toDate");
  }

  const response = await smartApiPriceService.getHistoricalCandleData({
    exchange,
    symboltoken,
    interval: "ONE_DAY",
    fromdate: toSmartApiDate(fromDate, "09:15"),
    todate: toSmartApiDate(toDate, "15:30"),
  });
  console.log("SmartAPI Response:", response);

  if (!response || !Array.isArray(response.data)) {
    throw new Error("Invalid SmartAPI response");
  }

  const savedCandles = [];
  let skippedCandles = 0;

  for (const candle of response.data) {
    const [timestamp, open, high, low, close, volume] = candle;
    const date = safeNormalizeEodDate(timestamp);
    if (!date) {
      skippedCandles += 1;
      continue;
    }

    const doc = {
      master_id,
      symbol,
      date,
      exchange,
      open,
      high,
      low,
      close,
      volume,
      source: "smartapi",
    };

    savedCandles.push(doc);
  }

  if (savedCandles.length) {
    await createEodForAllStocks(savedCandles);
  }

  if (skippedCandles) {
    console.warn(
      `Skipped ${skippedCandles} invalid EOD candles for master_id=${master_id}, symbol=${symbol}`,
    );
  }

  return savedCandles;
}

function toIsoDateOnly(date) {
  const parsed = new Date(date);
  if (Number.isNaN(parsed.getTime())) {
    throw new RangeError(`Invalid date value for toIsoDateOnly: ${date}`);
  }
  return parsed.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const d = new Date(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function buildFiveYearDateRanges(fromDate, toDate) {
  const ranges = [];
  let cursor = new Date(`${fromDate}T00:00:00.000Z`);
  const end = new Date(`${toDate}T00:00:00.000Z`);

  while (cursor <= end) {
    const chunkStart = new Date(cursor);
    const chunkEnd = new Date(cursor);
    chunkEnd.setUTCFullYear(chunkEnd.getUTCFullYear() + 5);
    chunkEnd.setUTCDate(chunkEnd.getUTCDate() - 1);

    if (chunkEnd > end) {
      chunkEnd.setTime(end.getTime());
    }

    ranges.push({
      fromDate: toIsoDateOnly(chunkStart),
      toDate: toIsoDateOnly(chunkEnd),
    });

    cursor = addDays(chunkEnd, 1);
  }

  return ranges;
}

async function fetchEodByMasterIdRangeChunked({
  master_id,
  symboltoken,
  fromDate,
  toDate,
  exchange = "NSE",
  symbol,
}) {
  if (!master_id || !symboltoken || !fromDate || !toDate || !symbol) {
    throw new Error("master_id, symboltoken, fromDate, toDate are required");
  }

  const from = new Date(`${fromDate}T00:00:00.000Z`);
  const to = new Date(`${toDate}T00:00:00.000Z`);
  if (from > to) {
    throw new Error("fromDate must be less than or equal to toDate");
  }

  const latestStoredDate = await eodRepo.getLatestTradeDateByMasterId(master_id);
  let effectiveFrom = fromDate;

  if (latestStoredDate) {
    const latestStoredDateIso = toIsoDateOnly(latestStoredDate);
    const nextMissingDate = addDays(new Date(`${latestStoredDateIso}T00:00:00.000Z`), 1);
    const nextMissingDateIso = toIsoDateOnly(nextMissingDate);
    if (nextMissingDateIso > effectiveFrom) {
      effectiveFrom = nextMissingDateIso;
    }
  }

  if (effectiveFrom > toDate) {
    return {
      count: 0,
      data: [],
      chunks: [],
      latestStoredDate,
      effectiveFromDate: effectiveFrom,
      message: "No new EOD range left to fetch",
    };
  }

  const ranges = buildFiveYearDateRanges(effectiveFrom, toDate);
  const savedCandles = [];
  const chunks = [];

  for (const range of ranges) {
    const response = await smartApiPriceService.getHistoricalCandleData({
      exchange,
      symboltoken,
      interval: "ONE_DAY",
      fromdate: toSmartApiDate(range.fromDate, "09:15"),
      todate: toSmartApiDate(range.toDate, "15:30"),
    });

    if (!response || !Array.isArray(response.data)) {
      chunks.push({
        fromDate: range.fromDate,
        toDate: range.toDate,
        fetched: 0,
      });
      continue;
    }

    const chunkCandles = [];
    let skippedCandles = 0;
    for (const candle of response.data) {
      const [timestamp, open, high, low, close, volume] = candle;
      const date = safeNormalizeEodDate(timestamp);
      if (!date) {
        skippedCandles += 1;
        continue;
      }
      chunkCandles.push({
        master_id,
        symbol,
        date,
        exchange,
        open,
        high,
        low,
        close,
        volume,
        source: "smartapi",
      });
    }

    if (chunkCandles.length) {
      await createEodForAllStocks(chunkCandles);
      savedCandles.push(...chunkCandles);
    }

    if (skippedCandles) {
      console.warn(
        `Skipped ${skippedCandles} invalid EOD candles for master_id=${master_id}, symbol=${symbol}, chunk=${range.fromDate}..${range.toDate}`,
      );
    }

    chunks.push({
      fromDate: range.fromDate,
      toDate: range.toDate,
      fetched: chunkCandles.length,
    });
  }

  return {
    count: savedCandles.length,
    data: savedCandles,
    chunks,
    latestStoredDate,
    effectiveFromDate: effectiveFrom,
  };
}

async function getEodByMasterIdRangeFromDb({
  master_id,
  fromDate,
  toDate,
  limit = 5000,
}) {
  if (!master_id) {
    throw new Error("master_id is required");
  }

  if (fromDate && toDate) {
    const from = new Date(`${fromDate}T00:00:00.000Z`);
    const to = new Date(`${toDate}T00:00:00.000Z`);
    if (from > to) {
      throw new Error("fromDate must be less than or equal to toDate");
    }
  }

  return eodRepo.listDailyCandlesByMasterIdRange({
    master_id,
    fromDate,
    toDate,
    limit,
  });
}

module.exports = {
  createOhlcEod,
  createEodForAllStocks,
  fetchEodByMasterIdRange,
  fetchEodByMasterIdRangeChunked,
  getEodByMasterIdRangeFromDb,
};
