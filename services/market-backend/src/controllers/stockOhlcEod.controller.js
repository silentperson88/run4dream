const {
  fetchEodByMasterIdRange,
  fetchEodByMasterIdRangeChunked,
  getEodByMasterIdRangeFromDb,
} = require("../services/stockOhlcEod.service");
const masterService = require("../services/stockMaster.service");

function toIsoDateOnly(value) {
  return new Date(value).toISOString().slice(0, 10);
}

function getFetchedCoverage(candles = []) {
  if (!Array.isArray(candles) || candles.length === 0) {
    return { actualFromDate: null, actualToDate: null };
  }

  let minDate = null;
  let maxDate = null;

  for (const candle of candles) {
    const isoDate = toIsoDateOnly(candle?.date);
    if (!minDate || isoDate < minDate) minDate = isoDate;
    if (!maxDate || isoDate > maxDate) maxDate = isoDate;
  }

  return { actualFromDate: minDate, actualToDate: maxDate };
}

async function fetchEodByRange(req, res) {
  try {
    console.log("fetchEodByRange");
    const { master_id, fromDate, toDate } = req.body;

    // get symbol and exchange from master table
    const master = await masterService.getMasterStockById(master_id);

    if (!master) {
      return res.status(400).json({
        success: false,
        message: "Invalid master_id",
      });
    }
    console.log(" master:", master);

    const data = await fetchEodByMasterIdRange({
      master_id,
      symboltoken: master.token,
      symbol: master.symbol,
      fromDate,
      toDate,
      exchange: master.exchange,
    });

    console.log("EOD Range Fetch Success:", data, "records");

    const coverage = getFetchedCoverage(data);
    await masterService.updateHistoryCoverage(master_id, {
      requestedFromDate: fromDate,
      requestedToDate: toDate,
      actualFromDate: coverage.actualFromDate,
      actualToDate: coverage.actualToDate,
    });

    res.json({
      success: true,
      count: data.length,
      data,
    });
  } catch (err) {
    console.error("EOD Range Fetch Error:", err);
    res.status(500).json({
      success: false,
      message: err.message,
    });
  }
}

async function fetchEodByRangeChunked(req, res) {
  try {
    console.log("fetchEodByRangeChunked");
    const { master_id, fromDate, toDate } = req.body;

    const master = await masterService.getMasterStockById(master_id);
    if (!master) {
      return res.status(400).json({
        success: false,
        message: "Invalid master_id",
      });
    }

    const result = await fetchEodByMasterIdRangeChunked({
      master_id,
      symboltoken: master.token,
      symbol: master.symbol,
      fromDate,
      toDate,
      exchange: master.exchange,
    });

    const coverage = getFetchedCoverage(result.data);
    await masterService.updateHistoryCoverage(master_id, {
      requestedFromDate: fromDate,
      requestedToDate: toDate,
      actualFromDate: coverage.actualFromDate,
      actualToDate: coverage.actualToDate,
    });

    return res.json({
      success: true,
      count: result.count,
      chunks: result.chunks,
      latestStoredDate: result.latestStoredDate,
      effectiveFromDate: result.effectiveFromDate,
      message: result.message,
      data: result.data,
    });
  } catch (err) {
    console.error("EOD Range Chunked Fetch Error:", err);
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
}

async function getEodFromDbByRange(req, res) {
  try {
    const { master_id } = req.params;
    const { fromDate, toDate } = req.query;
    const limit = req.query.limit ? Number(req.query.limit) : 5000;

    const data = await getEodByMasterIdRangeFromDb({
      master_id,
      fromDate,
      toDate,
      limit,
    });

    return res.json({
      success: true,
      count: data.length,
      data,
    });
  } catch (err) {
    console.error("Get EOD From DB Error:", err);
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
}

module.exports = {
  fetchEodByRange,
  fetchEodByRangeChunked,
  getEodFromDbByRange,
};
