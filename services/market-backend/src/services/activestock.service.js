const activeStocksRepo = require("../repositories/activeStocks.repository");
const stockMasterRepo = require("../repositories/stockMaster.repository");
const eodRepo = require("../repositories/eod.repository");

exports.addStock = async (stockData, db) => {
  if (
    !stockData?.exchange ||
    !stockData?.symbol ||
    !stockData?.master_id
  ) {
    throw new Error("Invalid stock data");
  }

  try {
    return await activeStocksRepo.create(stockData, db);
  } catch (err) {
    if (err.code === "23505") {
      throw new Error("Stock already active");
    }
    throw err;
  }
};

exports.getActiveStocks = async (page = 1, limit = 50, search = "", options = {}) => {
  const rows = await activeStocksRepo.listActive({ page, limit, search });
  const asOfDate = options?.asOfDate || null;
  if (!asOfDate) return rows;

  const candleRows = await eodRepo.getLatestCandleRowsByMasterIds(rows.map((row) => row.master_id), asOfDate);
  const candleByMasterId = new Map(candleRows.map((row) => [Number(row.master_id), row]));

  return rows.map((row) => {
    const candle = candleByMasterId.get(Number(row.master_id));
    if (!candle) return row;

    return {
      ...row,
      ltp: candle.close ?? row.ltp,
      open: candle.open ?? row.open,
      high: candle.high ?? row.high,
      low: candle.low ?? row.low,
      close: candle.close ?? row.close,
      volume: candle.volume ?? row.volume,
      updated_at: candle.trade_date || row.updated_at,
    };
  });
};

exports.getAllActiveStocks = async () => activeStocksRepo.listActive();

exports.getActiveStocksSnapshotByMasterIds = async (masterIds = [], { asOfDate = null } = {}) => {
  const rows = await activeStocksRepo.listByMasterIds(masterIds);
  if (!rows.length) return [];
  if (!asOfDate) return rows;

  const candleRows = await eodRepo.getLatestCandleRowsByMasterIds(
    rows.map((row) => row.master_id),
    asOfDate,
  );
  const candleByMasterId = new Map(candleRows.map((row) => [Number(row.master_id), row]));

  return rows.map((row) => {
    const candle = candleByMasterId.get(Number(row.master_id));
    return {
      ...row,
      trade_date: candle?.trade_date || null,
      ltp: candle?.close ?? row.ltp,
      open: candle?.open ?? row.open,
      high: candle?.high ?? row.high,
      low: candle?.low ?? row.low,
      close: candle?.close ?? row.close,
      volume: candle?.volume ?? 0,
      dma_20: candle?.dma_20 ?? null,
      dma_50: candle?.dma_50 ?? null,
      dma_200: candle?.dma_200 ?? null,
      atr_14: candle?.atr_14 ?? null,
      rsi_14: candle?.rsi_14 ?? null,
      adx_14: candle?.adx_14 ?? null,
      supertrend_signal: candle?.supertrend_signal ?? null,
      is_liquid: candle?.is_liquid ?? null,
      return_1m: candle?.return_1m ?? null,
      week_52_high: candle?.week_52_high ?? null,
      week_52_low: candle?.week_52_low ?? null,
    };
  });
};

exports.getBacktestAnalyticsByMasterIds = async (masterIds = [], { fromDate = null, toDate = null } = {}) => {
  const ids = Array.from(new Set((masterIds || []).map(Number).filter(value => Number.isFinite(value) && value > 0)));
  if (!ids.length || !toDate) return [];

  const rowsByMaster = await Promise.all(
    ids.map(async masterId => {
      const candles = await eodRepo.listDailyCandlesByMasterIdRange({
        master_id: masterId,
        fromDate,
        toDate,
      });

      if (!candles.length) {
        return {
          master_id: masterId,
          candle_count: 0,
          peak_price: null,
          peak_trade_date: null,
          latest_trade_date: null,
          latest_close: null,
          latest_atr_14: null,
          latest_dma_50: null,
          latest_dma_200: null,
          latest_rsi_14: null,
          latest_adx_14: null,
          latest_supertrend_signal: null,
        };
      }

      const latest = candles[candles.length - 1];
      const peakCandle = candles.reduce((best, candle) => {
        const candleHigh = Number(candle?.high ?? candle?.close ?? 0);
        const bestHigh = Number(best?.high ?? best?.close ?? 0);
        return candleHigh > bestHigh ? candle : best;
      }, candles[0]);

      return {
        master_id: masterId,
        candle_count: candles.length,
        peak_price: Number(peakCandle?.high ?? peakCandle?.close ?? 0),
        peak_trade_date: peakCandle?.trade_date || null,
        latest_trade_date: latest?.trade_date || null,
        latest_close: Number(latest?.close ?? 0),
        latest_atr_14: latest?.atr_14 ?? null,
        latest_dma_50: latest?.dma_50 ?? null,
        latest_dma_200: latest?.dma_200 ?? null,
        latest_rsi_14: latest?.rsi_14 ?? null,
        latest_adx_14: latest?.adx_14 ?? null,
        latest_supertrend_signal: latest?.supertrend_signal ?? null,
      };
    }),
  );

  return rowsByMaster;
};

exports.deactivateStock = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) return null;
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master) return null;
  return stockMasterRepo.updateById(master.id, { is_active: false });
};

exports.getActiveStockByToken = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) throw new Error("Active stock not found");
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master?.is_active) throw new Error("Active stock not found");
  return stock;
};

exports.getActiveStockByMasterId = async (masterId) => {
  const stock = await activeStocksRepo.getByMasterId(masterId);
  if (!stock) return null;
  const master = await stockMasterRepo.getById(masterId);
  if (!master?.is_active) return null;
  return stock;
};

exports.updateActiveStockPrice = async (token, priceData) => {
  const updatedStock = await activeStocksRepo.updateByToken(token, {
    ltp: priceData.ltp,
    open: priceData.open,
    high: priceData.high,
    low: priceData.low,
    close: priceData.close,
    percentChange: priceData.percentChange,
    avgPrice: priceData.avgPrice,
    lowerCircuit: priceData.lowerCircuit,
    upperCircuit: priceData.upperCircuit,
    week52Low: priceData.week52Low,
    week52High: priceData.week52High,
    updatedAt: new Date(),
  });

  if (!updatedStock) throw new Error("Active stock not found");
  return updatedStock;
};

exports.bulkUpdateStocksInFullMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, [
    "ltp",
    "open",
    "high",
    "low",
    "close",
    "percentChange",
    "avgPrice",
    "lowerCircuit",
    "upperCircuit",
    "week52Low",
    "week52High",
  ]);
};

exports.bulkUpdateStocksInLTPMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, ["ltp"]);
};

exports.bulkUpdateStocksInOHLCMode = async (stocks) => {
  if (!stocks || !stocks.length) return;
  await activeStocksRepo.bulkUpsertByToken(stocks, [
    "ltp",
    "open",
    "high",
    "low",
    "close",
  ]);
};

exports.toggleActiveStock = async (token) => {
  const stock = await activeStocksRepo.getByToken(token);
  if (!stock) throw new Error("Active stock not found");
  const master = await stockMasterRepo.getById(stock.master_id);
  if (!master) throw new Error("Active stock not found");

  const updatedMaster = await stockMasterRepo.updateById(master.id, {
    is_active: !Boolean(master.is_active),
  });

  if (!updatedMaster) throw new Error("Active stock not found");

  return {
    ...stock,
    master_is_active: updatedMaster.is_active,
  };
};

exports.deleteActiveStock = async (token) => {
  const deleted = await activeStocksRepo.deleteByToken(token);
  if (!deleted) throw new Error("Active stock not found");
};

exports.getActiveStocksByMasterIds = async (masterIds = []) =>
  activeStocksRepo.listByMasterIds(masterIds);
