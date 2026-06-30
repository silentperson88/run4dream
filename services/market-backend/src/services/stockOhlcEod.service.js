const eodRepo = require("../repositories/eod.repository");
const { SmartApiPriceService } = require("./smartapi.service");
const { toSmartApiDate, normalizeEodDate } = require("../utils/Mthods.utils");
const { computeDerivedMetricsForCandles } = require("./eodDerivedMetrics.service");
const activeStockService = require("./activestock.service");
const stockMasterService = require("./stockMaster.service");

const smartApiPriceService = new SmartApiPriceService();
const FULL_MODE_BATCH_SIZE = 50;
const FULL_MODE_EOD_DELAY_MS = Number(process.env.FULL_MODE_EOD_DELAY_MS || 1000);

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

async function refreshDerivedMetricsForMasterId(masterId) {
  const updated = await refreshDerivedMetricsForMasterIds([masterId]);
  return updated;
}

async function refreshDerivedMetricsForMasterIds(masterIds = []) {
  const ids = Array.from(new Set((masterIds || []).map(Number).filter((value) => Number.isFinite(value) && value > 0)));
  if (!ids.length) return 0;

  const candles = await eodRepo.listAllCandlesByMasterIds(ids);
  if (!candles.length) return 0;

  const grouped = new Map();
  for (const candle of candles) {
    const masterId = Number(candle.master_id);
    if (!grouped.has(masterId)) grouped.set(masterId, []);
    grouped.get(masterId).push(candle);
  }

  const computed = [];
  for (const [masterId, stockCandles] of grouped.entries()) {
    const rows = computeDerivedMetricsForCandles(stockCandles);
    if (rows.length) computed.push(...rows);
  }
  if (!computed.length) return 0;

  return eodRepo.bulkUpdateDerivedMetrics(computed);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

const isMarketClosedInIndia = () => {
  const now = getIndiaNowParts();
  return now.hour > 15 || (now.hour === 15 && now.minute >= 30);
};

const chunk = (items = [], size = FULL_MODE_BATCH_SIZE) => {
  const batches = [];
  for (let index = 0; index < items.length; index += size) {
    batches.push(items.slice(index, index + size));
  }
  return batches;
};

const normalizeToken = (value) => String(value || "").trim();

const toNumberOrNull = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const toIndiaDateOnly = (value) => {
  if (!value) return null;
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(parsed);
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return year && month && day ? `${year}-${month}-${day}` : null;
};

const resolveTradeDateFromFullItem = (item = {}, fallbackTradeDate) => {
  const candidates = [
    item.exchFeedTime,
    item.exchangeFeedTime,
    item.lastTradeTime,
    item.tradeTime,
    item.feedTime,
    item.updatedAt,
  ];

  for (const candidate of candidates) {
    const isoDate = toIndiaDateOnly(candidate);
    if (isoDate) return isoDate;
  }

  return fallbackTradeDate;
};

const mapSmartApiFullToActivePayload = (item = {}) => ({
  symbol: item.tradingSymbol || null,
  token: normalizeToken(item.symbolToken || item.symboltoken || item.token),
  ltp: Number(item.ltp) || 0,
  open: Number(item.open) || 0,
  high: Number(item.high) || 0,
  low: Number(item.low) || 0,
  close: Number(item.close ?? item.ltp) || 0,
  volume: Number(item.volume) || 0,
  percentChange: Number(item.percentChange ?? item.change ?? item.netChange) || 0,
  avgPrice: Number(item.avgPrice ?? item.averagePrice) || 0,
  lowerCircuit: Number(item.lowerCircuit) || 0,
  upperCircuit: Number(item.upperCircuit) || 0,
  week52Low: Number(item.week52Low || item["52WeekLow"]) || 0,
  week52High: Number(item.week52High || item["52WeekHigh"]) || 0,
});

async function syncDailyEodFromFullMode({ masterId = null } = {}) {
  if (!isMarketClosedInIndia()) {
    throw new Error("FULL mode daily EOD sync can run only after market close (3:30 PM IST)");
  }

  const tradeDate = getIndiaNowParts().dateIso;
  const eligibleMasters = await stockMasterService.getEligibleDailyFullEodMasters(
    masterId ? [Number(masterId)] : null,
  );
  const selectedStocks = Array.isArray(eligibleMasters)
    ? eligibleMasters.filter((row) => {
        if (!row?.id || !row?.token || !row?.symbol) return false;
        return true;
      })
    : [];

  if (!selectedStocks.length) {
    return {
      tradeDate,
      totalCandidates: 0,
      totalFetched: 0,
      totalSaved: 0,
      totalDerivedUpdated: 0,
      batches: 0,
      rows: [],
    };
  }

  const byExchange = new Map();
  for (const row of selectedStocks) {
    const exchange = String(row.exchange || "NSE").toUpperCase();
    if (!byExchange.has(exchange)) byExchange.set(exchange, []);
    byExchange.get(exchange).push(row);
  }

  const exchangeBatches = [];
  for (const [exchange, rows] of byExchange.entries()) {
    for (const batch of chunk(rows, FULL_MODE_BATCH_SIZE)) {
      exchangeBatches.push({ exchange, rows: batch });
    }
  }

  const syncedRows = [];
  let totalFetched = 0;
  let totalSaved = 0;
  let totalDerivedUpdated = 0;

  for (let index = 0; index < exchangeBatches.length; index += 1) {
    const batch = exchangeBatches[index];
    const tokenIds = batch.rows.map((row) => normalizeToken(row.token)).filter(Boolean);
    if (!tokenIds.length) continue;

    const response = await smartApiPriceService.getMarketData("FULL", tokenIds, batch.exchange);
    const payload = response?.data || {};
    const fetched = Array.isArray(payload?.fetched) ? payload.fetched : [];
    if (!fetched.length) {
      if (index < exchangeBatches.length - 1) await sleep(FULL_MODE_EOD_DELAY_MS);
      continue;
    }

    const batchByToken = new Map(batch.rows.map((row) => [normalizeToken(row.token), row]));
    const activePayload = [];
    const eodPayload = [];
    const derivedMasterIds = new Set();

    for (const item of fetched) {
      const token = normalizeToken(item?.symbolToken ?? item?.symboltoken ?? item?.token);
      const baseRow = batchByToken.get(token);
      if (!baseRow) continue;

      const activePayloadRow = mapSmartApiFullToActivePayload(item);
      activePayload.push(activePayloadRow);

      eodPayload.push({
        master_id: Number(baseRow.id),
        symbol: baseRow.symbol,
        exchange: String(baseRow.exchange || batch.exchange || "NSE").toUpperCase(),
        date: resolveTradeDateFromFullItem(item, tradeDate),
        open: activePayloadRow.open,
        high: activePayloadRow.high,
        low: activePayloadRow.low,
        close: activePayloadRow.close,
        ltp: activePayloadRow.ltp,
        volume: activePayloadRow.volume,
        lower_circuit: activePayloadRow.lowerCircuit,
        upper_circuit: activePayloadRow.upperCircuit,
        source: "smartapi_full",
      });

      derivedMasterIds.add(Number(baseRow.id));
    }

    if (activePayload.length) {
      await activeStockService.bulkUpdateStocksInFullMode(activePayload);
    }

    if (eodPayload.length) {
      const saveResult = await createEodForAllStocks(eodPayload);
      totalSaved += Number(saveResult?.insertedCount || 0);
      totalDerivedUpdated += await refreshDerivedMetricsForMasterIds(Array.from(derivedMasterIds));
      syncedRows.push(...eodPayload);
    }

    totalFetched += fetched.length;

    if (index < exchangeBatches.length - 1) {
      await sleep(FULL_MODE_EOD_DELAY_MS);
    }
  }

  return {
    tradeDate,
    totalCandidates: selectedStocks.length,
    totalFetched,
    totalSaved,
    totalDerivedUpdated,
    batches: exchangeBatches.length,
    rows: syncedRows,
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
    await refreshDerivedMetricsForMasterId(master_id);
  }

  if (skippedCandles) {
    console.warn(
      `Skipped ${skippedCandles} invalid EOD candles for master_id=${master_id}, symbol=${symbol}`,
    );
  }

  return savedCandles;
}

async function fetchEodByMasterIdRangePreview({
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

  if (!response || !Array.isArray(response.data)) {
    throw new Error("Invalid SmartAPI response");
  }

  const rawCandles = response.data;
  const candles = [];
  let skippedCandles = 0;

  for (const candle of rawCandles) {
    const [timestamp, open, high, low, close, volume] = candle;
    const date = safeNormalizeEodDate(timestamp);
    if (!date) {
      skippedCandles += 1;
      continue;
    }

    candles.push({
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

  if (skippedCandles) {
    console.warn(
      `Skipped ${skippedCandles} invalid preview EOD candles for master_id=${master_id}, symbol=${symbol}`,
    );
  }

  return {
    rawCandles,
    candles,
  };
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

  if (savedCandles.length) {
    await refreshDerivedMetricsForMasterId(master_id);
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
  syncDailyEodFromFullMode,
  fetchEodByMasterIdRange,
  fetchEodByMasterIdRangePreview,
  fetchEodByMasterIdRangeChunked,
  getEodByMasterIdRangeFromDb,
};
