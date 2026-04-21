const stockFundamentalsService = require("../services/stockFundamental.service");
const dividendAnalysisService = require("../services/dividendAnalysis.service");
const growthAnalysisService = require("../services/growthAnalysis.service");
const garpAnalysisService = require("../services/garpAnalysis.service");
const valueAnalysisService = require("../services/valueAnalysis.service");
const pivotAnalysisService = require("../services/pivotAnalysis.service");
const stockSearchService = require("../services/stockSearch.service");
const stockMasterService = require("../services/stockMaster.service");
const activeStockService = require("../services/activestock.service");
const fs = require("fs/promises");
const path = require("path");

const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");
const { getAsOfDateFromRequest, filterRowsByAsOfDate } = require("../utils/asOfDate.utils");
const { pythonApi } = require("../pythonApi/apiService.py");
const { PYTHON_ENDPOINTS } = require("../pythonApi/endpoints.py");
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const { enqueueFundamentalsJobs } = require("../schedulers/fundamentals.scheduler");
const { scrapeWithFallback } = require("../services/fundamentalsScrape.service");
const { buildMappedFundamentals } = require("../services/fundamentalsMapper.service");
const stockTechnicalService = require("../services/stockTechnical.service");
const eodRepository = require("../repositories/eod.repository");

const normalizeTextArray = (value) => {
  if (!Array.isArray(value)) return [];

  return value
    .map((item) => {
      if (typeof item === "string") return item;
      if (item && typeof item === "object") {
        const values = Object.values(item);
        const firstString = values.find((v) => typeof v === "string");
        return firstString || null;
      }
      return null;
    })
    .filter(Boolean);
};

const toNumberOrNull = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildHistoricalOverviewMarketSnapshot = async (masterId, fallbackSnapshot = {}, asOfDate = null) => {
  if (!masterId || !asOfDate) {
    return {
      marketSnapshot: fallbackSnapshot,
      latestTradeDate: null,
    };
  }

  const [latestRows, recentCandles] = await Promise.all([
    eodRepository.getLatestCandleRowsByMasterIds([masterId], asOfDate),
    eodRepository.listRecentCandlesByMasterIds([masterId], { limitPerMaster: 252, asOfDate }),
  ]);

  const latest = Array.isArray(latestRows) ? latestRows[0] : null;
  const candles = Array.isArray(recentCandles) ? recentCandles : [];
  if (!latest || !candles.length) {
    return {
      marketSnapshot: fallbackSnapshot,
      latestTradeDate: null,
    };
  }

  const closes = candles.map((row) => toNumberOrNull(row.close)).filter((value) => value !== null);
  const highs = candles.map((row) => toNumberOrNull(row.high)).filter((value) => value !== null);
  const lows = candles.map((row) => toNumberOrNull(row.low)).filter((value) => value !== null);

  const week52High = highs.length ? Math.max(...highs) : toNumberOrNull(fallbackSnapshot?.week52_high);
  const week52Low = lows.length ? Math.min(...lows) : toNumberOrNull(fallbackSnapshot?.week52_low);

  return {
    marketSnapshot: {
      ...fallbackSnapshot,
      current_price: toNumberOrNull(latest.close) ?? fallbackSnapshot?.current_price ?? null,
      high_low:
        week52High !== null && week52Low !== null
          ? `${week52High} / ${week52Low}`
          : fallbackSnapshot?.high_low || null,
      week52_high: week52High,
      week52_low: week52Low,
    },
    latestTradeDate: latest.trade_date || null,
  };
};

const MONTH_MAP = {
  jan: 0,
  january: 0,
  feb: 1,
  february: 1,
  mar: 2,
  march: 2,
  apr: 3,
  april: 3,
  may: 4,
  jun: 5,
  june: 5,
  jul: 6,
  july: 6,
  aug: 7,
  august: 7,
  sep: 8,
  sept: 8,
  september: 8,
  oct: 9,
  october: 9,
  nov: 10,
  november: 10,
  dec: 11,
  december: 11,
};

const parseLegacyHeaderDate = (header) => {
  const text = String(header || "").trim();
  if (!text) return null;

  const normalized = text.toLowerCase().replace(/\s+/g, " ").trim();
  if (["ttm", "latest", "current", "today", "now"].includes(normalized)) {
    return "__DROP_FOR_HISTORICAL__";
  }

  const monthYearMatch = normalized.match(
    /^(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)[\s-]+(\d{2,4})$/,
  );
  if (monthYearMatch) {
    const monthIndex = MONTH_MAP[monthYearMatch[1]];
    const yearRaw = Number(monthYearMatch[2]);
    const year = yearRaw < 100 ? 2000 + yearRaw : yearRaw;
    const date = new Date(Date.UTC(year, monthIndex + 1, 0));
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
  }

  const yearMatch = normalized.match(/^(\d{4})$/);
  if (yearMatch) {
    const year = Number(yearMatch[1]);
    const date = new Date(Date.UTC(year, 11, 31));
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
  }

  return null;
};

const trimLegacyRowNodeValues = (node, indexes = []) => {
  if (!node || typeof node !== "object") return node;

  const nextNode = {
    ...node,
    values: Array.isArray(node.values) ? indexes.map((index) => node.values[index]) : node.values,
  };

  if (Array.isArray(node.children)) {
    nextNode.children = node.children.map((child) => trimLegacyRowNodeValues(child, indexes));
  }

  return nextNode;
};

const trimLegacyMappedTableByAsOfDate = (table, asOfDate) => {
  if (!asOfDate || !table || typeof table !== "object" || !Array.isArray(table.headers)) return table;

  const keepIndexes = table.headers.reduce((acc, header, index) => {
    const parsed = parseLegacyHeaderDate(header);
    if (parsed === "__DROP_FOR_HISTORICAL__") return acc;
    if (parsed === null || parsed <= asOfDate) acc.push(index);
    return acc;
  }, []);

  if (keepIndexes.length === table.headers.length) return table;

  const nextRows = Object.fromEntries(
    Object.entries(table.rows || {}).map(([key, value]) => [key, trimLegacyRowNodeValues(value, keepIndexes)]),
  );

  const nextUnmatchedRows = Array.isArray(table.unmatched_rows)
    ? table.unmatched_rows.map((item) => {
        const nextItem = { ...(item || {}) };
        table.headers.forEach((header, index) => {
          if (!keepIndexes.includes(index)) delete nextItem[header];
        });
        return nextItem;
      })
    : table.unmatched_rows;

  return {
    ...table,
    headers: keepIndexes.map((index) => table.headers[index]),
    rows: nextRows,
    unmatched_rows: nextUnmatchedRows,
  };
};

const trimLegacyFundamentalsTables = (data, asOfDate) => {
  if (!asOfDate || !data || typeof data !== "object") return data;

  const tables = data.tables && typeof data.tables === "object" ? data.tables : {};
  return {
    ...data,
    tables: {
      ...tables,
      quarters: trimLegacyMappedTableByAsOfDate(tables.quarters, asOfDate),
      profit_loss: trimLegacyMappedTableByAsOfDate(tables.profit_loss, asOfDate),
      balance_sheet: trimLegacyMappedTableByAsOfDate(tables.balance_sheet, asOfDate),
      cash_flow: trimLegacyMappedTableByAsOfDate(tables.cash_flow, asOfDate),
      ratios: trimLegacyMappedTableByAsOfDate(tables.ratios, asOfDate),
      shareholdings: trimLegacyMappedTableByAsOfDate(tables.shareholdings, asOfDate),
    },
  };
};

const TABLE_ROW_CATALOG_PATH = path.join(
  __dirname,
  "../../tmp/fundamentals/table-row-catalog.json",
);

const getRowLabel = (row, headers = []) => {
  if (!row || typeof row !== "object") return null;
  if (typeof row.label === "string" && row.label.trim()) return row.label.trim();
  if (headers.length && typeof row[headers[0]] === "string") {
    const value = row[headers[0]].trim();
    return value || null;
  }
  return null;
};

const collectRowKeys = (row) => {
  if (!row || typeof row !== "object") return [];
  return Object.keys(row)
    .filter((key) => key !== "children")
    .sort();
};

const buildTableRowCatalog = (tableNode, sectionKey) => {
  const table = tableNode?.main_table || null;
  if (!table) {
    return {
      section: sectionKey,
      exists: false,
      title: null,
      headers: [],
      row_labels: [],
      child_row_labels: [],
      row_shapes: [],
    };
  }

  const headers = Array.isArray(table.headers) ? table.headers : [];
  const rows = Array.isArray(table.rows) ? table.rows : [];
  const rowLabels = new Set();
  const childLabels = new Set();
  const rowShapes = new Set();

  rows.forEach((row) => {
    const label = getRowLabel(row, headers);
    if (label) rowLabels.add(label);

    const shape = collectRowKeys(row).join("|");
    if (shape) rowShapes.add(shape);

    const children = Array.isArray(row.children) ? row.children : [];
    children.forEach((child) => {
      const childLabel = getRowLabel(child, headers);
      if (childLabel) childLabels.add(childLabel);
      const childShape = collectRowKeys(child).join("|");
      if (childShape) rowShapes.add(childShape);
    });
  });

  return {
    section: sectionKey,
    exists: true,
    title: table.title || null,
    headers,
    row_labels: Array.from(rowLabels).sort(),
    child_row_labels: Array.from(childLabels).sort(),
    row_shapes: Array.from(rowShapes).sort(),
  };
};

const buildProfitLossOtherCatalog = (otherTables) => {
  const tables = Array.isArray(otherTables) ? otherTables : [];
  return tables.map((table, index) => {
    const headers = Array.isArray(table?.headers) ? table.headers : [];
    const rows = Array.isArray(table?.rows) ? table.rows : [];
    const rowLabels = new Set();
    const rowShapes = new Set();

    rows.forEach((row) => {
      const label = getRowLabel(row, headers);
      if (label) rowLabels.add(label);
      const shape = collectRowKeys(row).join("|");
      if (shape) rowShapes.add(shape);
    });

    return {
      section: "profit_loss.other_details",
      index,
      title: table?.title || null,
      headers,
      row_labels: Array.from(rowLabels).sort(),
      row_shapes: Array.from(rowShapes).sort(),
    };
  });
};

const buildOverviewFundamentalsPayload = async (masterStock, asOfDate = null) => {
  const [overviewRow, fullFundamentals] = await Promise.all([
    stockFundamentalsService.getOverviewFundamentalsByMasterId(masterStock.id),
    stockFundamentalsService.getFullStockFundamentals(masterStock.id).catch(() => null),
  ]);

  const baseCompanyInfo = fullFundamentals?.company_info || {};
  const baseSummary = fullFundamentals?.summary || {};
  const baseMarketSnapshot = baseSummary?.market_snapshot || {};

  if (!overviewRow && !fullFundamentals) return null;

  const technicals = await stockTechnicalService.getMomentumSnapshotByMasterId(masterStock.id).catch(() => null);
  const fallbackMarketSnapshot = {
    market_cap: overviewRow?.market_cap ?? baseMarketSnapshot?.market_cap ?? null,
    current_price: overviewRow?.current_price ?? baseMarketSnapshot?.current_price ?? null,
    high_low: overviewRow?.high_low ?? baseMarketSnapshot?.high_low ?? null,
    stock_pe: overviewRow?.stock_pe ?? baseMarketSnapshot?.stock_pe ?? baseMarketSnapshot?.pe_ratio ?? null,
    pe_ratio: overviewRow?.stock_pe ?? baseMarketSnapshot?.stock_pe ?? baseMarketSnapshot?.pe_ratio ?? null,
    book_value: overviewRow?.book_value ?? baseMarketSnapshot?.book_value ?? null,
    dividend_yield: overviewRow?.dividend_yield ?? baseMarketSnapshot?.dividend_yield ?? null,
    roce: overviewRow?.roce ?? baseMarketSnapshot?.roce ?? null,
    roe: overviewRow?.roe ?? baseMarketSnapshot?.roe ?? null,
    face_value: overviewRow?.face_value ?? baseMarketSnapshot?.face_value ?? null,
  };
  const { marketSnapshot, latestTradeDate } = await buildHistoricalOverviewMarketSnapshot(
    masterStock.id,
    fallbackMarketSnapshot,
    asOfDate,
  );

  return {
    master_id: String(masterStock.id),
    active_stock_id:
      (overviewRow?.active_stock_id ? String(overviewRow.active_stock_id) : null) ||
      (fullFundamentals?.active_stock_id ? String(fullFundamentals.active_stock_id) : null),
    company: overviewRow?.company_name || fullFundamentals?.company || baseCompanyInfo?.company_name || masterStock.name || null,
    company_info: {
      company_name: overviewRow?.company_name || baseCompanyInfo?.company_name || masterStock.name || null,
      about: overviewRow?.about || baseCompanyInfo?.about || null,
      key_points: overviewRow?.key_points || baseCompanyInfo?.key_points || null,
      links: Array.isArray(overviewRow?.links) ? overviewRow.links : Array.isArray(baseCompanyInfo?.links) ? baseCompanyInfo.links : [],
    },
    summary: {
      market_snapshot: marketSnapshot,
      pros: Array.isArray(overviewRow?.pros) ? overviewRow.pros : Array.isArray(baseSummary?.pros) ? baseSummary.pros : [],
      cons: Array.isArray(overviewRow?.cons) ? overviewRow.cons : Array.isArray(baseSummary?.cons) ? baseSummary.cons : [],
    },
    documents: {},
    technicals,
    as_of_date: asOfDate,
    eod_trade_date: latestTradeDate,
    last_updated_at:
      (asOfDate && latestTradeDate) ||
      overviewRow?.last_updated_at ||
      fullFundamentals?.last_updated_at ||
      masterStock.updated_at ||
      new Date().toISOString(),
  };
};

const writeFundamentalsTableRowCatalog = async (masterStock, data) => {
  const catalog = {
    generated_at: new Date().toISOString(),
    stock: {
      master_id: String(masterStock?.id || ""),
      name: masterStock?.name || null,
      symbol: masterStock?.symbol || null,
      screener_url: masterStock?.screener_url || null,
    },
    tables: {
      peers: buildTableRowCatalog(data?.peers, "peers"),
      quarters: buildTableRowCatalog(data?.quarters, "quarters"),
      profit_loss_main: buildTableRowCatalog(data?.profit_loss, "profit_loss.main_table"),
      profit_loss_other_details: buildProfitLossOtherCatalog(data?.profit_loss?.other_details),
      balance_sheet: buildTableRowCatalog(data?.balance_sheet, "balance_sheet"),
      cash_flow: buildTableRowCatalog(data?.cash_flow, "cash_flow"),
      ratios: buildTableRowCatalog(data?.ratios, "ratios"),
      shareholdings: buildTableRowCatalog(data?.shareholdings, "shareholdings"),
    },
  };

  const dir = path.dirname(TABLE_ROW_CATALOG_PATH);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(TABLE_ROW_CATALOG_PATH, JSON.stringify(catalog, null, 2), "utf8");
};

const upsertMappedFundamentals = async (masterStock, rawData) => {
  const masterId = Number(masterStock?.id);
  if (!masterId) return null;

  const existing = await stockFundamentalsService.getFullStockFundamentals(masterId);

  let activeStockId = existing?.active_stock_id || null;
  if (!activeStockId) {
    const active = await activeStockService.getActiveStockByMasterId(masterId);
    activeStockId = active?.id || null;
  }

  if (!activeStockId) {
    throw new Error("active_stock_id not found for master stock");
  }

  const mapped = buildMappedFundamentals(rawData);
  return stockFundamentalsService.upsertFundamentals({
    master_id: masterId,
    active_stock_id: activeStockId,
    company: mapped.company,
    company_info: mapped.company_info,
    summary: mapped.summary,
    peers: mapped.peers,
    tables: mapped.tables,
    other_details: mapped.other_details,
    documents: mapped.documents,
    raw_payload: mapped.raw_payload,
    last_updated_at: new Date(),
  });
};

exports.fetchStockFundamentals = async (req, res) => {
  try {
    const master_id = req.body?.master_id || "";

    if (!master_id) return response(res, 400, responseUtils.MASTER_ID_REQUIRED);

    const masterStock = await stockMasterService.getMasterStockById(master_id);
    if (!masterStock) return response(res, 400, responseUtils.STOCK_NOT_FOUND);
    if (!masterStock.is_active) {
      return response(res, 400, responseUtils.STOCK_INACTIVE);
    }
    if (String(masterStock.screener_status || "PENDING").toUpperCase() !== "PENDING") {
      return response(res, 400, responseUtils.SCREENER_STATUS_NOT_PENDING);
    }

      if (!masterStock.screener_url) {
        await stockMasterService.updateMasterStock(masterStock.id, {
        screener_status: "FAILED_NO_RETRY",
        }).catch(() => {});
        return response(res, 400, responseUtils.SCREENER_URL_NOT_EXIST);
      }

    const result = await pythonApi.post(
      PYTHON_ENDPOINTS.FETCH_STOCK_FUNDAMENTALS,
      { url: masterStock.screener_url },
    );

      if (result.status !== 200) {
        await stockMasterService.updateMasterStock(masterStock.id, {
        screener_status: "FAILED_NO_RETRY",
        }).catch(() => {});
        return response(
        res,
        400,
        responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS,
        { result },
      );
    }

    const stockDetails = result.data.data;

    const fundamentals = {
      master_id: Number(master_id),
      company: stockDetails.company,
      summary: {
        market_snapshot: stockDetails.market_snapshot || {},
        pros: normalizeTextArray(stockDetails.pros),
        cons: normalizeTextArray(stockDetails.cons),
      },
      financials: {
        quarterly_results: Array.isArray(stockDetails.quarterly_results)
          ? stockDetails.quarterly_results
          : [],
        yearly_pnl: Array.isArray(stockDetails.profit_and_loss_yearly)
          ? stockDetails.profit_and_loss_yearly
          : [],
      },
      statements: {
        balance_sheet: Array.isArray(stockDetails.balance_sheet)
          ? stockDetails.balance_sheet
          : [],
        cash_flows: Array.isArray(stockDetails.cash_flows)
          ? stockDetails.cash_flows
          : [],
      },
      ratios: Array.isArray(stockDetails.ratios) ? stockDetails.ratios : [],
    };

    const data = await stockFundamentalsService.updateEntry(fundamentals);
      if (!data) {
        await stockMasterService.updateMasterStock(masterStock.id, {
        screener_status: "FAILED_NO_RETRY",
        }).catch(() => {});
        return response(res, 400, responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS);
      }

    await stockMasterService.updateMasterStock(masterStock.id, {
      screener_status: "VALID",
    }).catch(() => {});
    return response(res, 200, responseUtils.SUCCESS, { data, stockDetails });
  } catch (error) {
    if (req.body?.master_id) {
      await stockMasterService.updateMasterStock(req.body.master_id, {
        screener_status: "FAILED_NO_RETRY",
      }).catch(() => {});
    }
    return response(
      res,
      400,
      responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS,
      error,
    );
  }
};

exports.getStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const [fundamentalsResult, momentumResult] = await Promise.allSettled([
      stockFundamentalsService.getFullStockFundamentals(master.id),
      stockTechnicalService.getMomentumSnapshotByMasterId(master.id),
    ]);

    const rawData = fundamentalsResult.status === "fulfilled" ? fundamentalsResult.value : null;
    const data = trimLegacyFundamentalsTables(rawData, asOfDate);
    if (!data) {
      return response(res, 400, responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS);
    }

    return response(res, 200, responseUtils.SUCCESS, {
      ...data,
      as_of_date: asOfDate,
      technicals: momentumResult.status === "fulfilled" ? momentumResult.value : null,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getOverviewStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const data = await buildOverviewFundamentalsPayload(master, asOfDate);
    if (!data) {
      return response(res, 400, responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS);
    }

    return response(res, 200, responseUtils.SUCCESS, data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getQuarterlyStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const rows = filterRowsByAsOfDate(await stockFundamentalsService.getQuarterlyFundamentalsBySymbol(symbol), asOfDate);
    return response(res, 200, responseUtils.SUCCESS, {
      symbol: master.symbol || symbol,
      company_name: master.name || null,
      master_id: String(master.id),
      active_stock_id: rows?.[0]?.active_stock_id ? String(rows[0].active_stock_id) : null,
      rows: Array.isArray(rows) ? rows : [],
      as_of_date: asOfDate,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

const buildSplitSectionResponse = async (symbol, section, asOfDate = null) => {
  const master = await stockMasterService.getMasterStockBySymbol(symbol);
  if (!master) return { status: 400, body: responseUtils.STOCK_NOT_FOUND };

  const getterMap = {
    profit_loss: stockFundamentalsService.getProfitLossFundamentalsBySymbol,
    balance_sheet: stockFundamentalsService.getBalanceSheetFundamentalsBySymbol,
    cash_flow: stockFundamentalsService.getCashFlowFundamentalsBySymbol,
    ratios: stockFundamentalsService.getRatiosFundamentalsBySymbol,
    shareholdings: stockFundamentalsService.getShareholdingFundamentalsBySymbol,
  };

  const rows = filterRowsByAsOfDate(await getterMap[section](symbol), asOfDate);
  return {
    status: 200,
    body: responseUtils.SUCCESS,
    data: {
      symbol: master.symbol || symbol,
      company_name: master.name || null,
      master_id: String(master.id),
      active_stock_id: rows?.[0]?.active_stock_id ? String(rows[0].active_stock_id) : null,
      rows: Array.isArray(rows) ? rows : [],
      as_of_date: asOfDate,
    },
  };
};

exports.getProfitLossStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await buildSplitSectionResponse(symbol, "profit_loss", asOfDate);
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getBalanceSheetStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await buildSplitSectionResponse(symbol, "balance_sheet", asOfDate);
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getCashFlowStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await buildSplitSectionResponse(symbol, "cash_flow", asOfDate);
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getRatiosStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await buildSplitSectionResponse(symbol, "ratios", asOfDate);
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getShareholdingStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await buildSplitSectionResponse(symbol, "shareholdings", asOfDate);
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getDividendAnalysis = async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 120)));
    const rows = await dividendAnalysisService.getDividendAnalysisRows({});
    const ranked = rows.map((row) => {
      const analysis = dividendAnalysisService.scoreDividendCandidate(row);
      return {
        ...row,
        analysis,
      };
    }).sort((a, b) => {
      const scoreDiff = Number(b?.analysis?.score || 0) - Number(a?.analysis?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const yieldDiff = Number(b?.analysis?.metrics?.dividend_yield || 0) - Number(a?.analysis?.metrics?.dividend_yield || 0);
      if (yieldDiff !== 0) return yieldDiff;
      return String(a?.symbol || "").localeCompare(String(b?.symbol || ""));
    }).slice(0, limit);

    return response(res, 200, responseUtils.SUCCESS, {
      limit,
      total: ranked.length,
      rows: ranked,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getDividendAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const row = await dividendAnalysisService.getDividendAnalysisBySymbol(symbol);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, {
      ...row,
      analysis: dividendAnalysisService.scoreDividendCandidate(row),
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getGrowthAnalysis = async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 120)));
    const rows = await growthAnalysisService.getGrowthAnalysisRows({ limit });
    const ranked = rows
      .map((row) => ({
        ...row,
        analysis: growthAnalysisService.scoreGrowthCandidate(row),
      }))
      .sort((a, b) => {
        const scoreDiff = Number(b?.analysis?.score || 0) - Number(a?.analysis?.score || 0);
        if (scoreDiff !== 0) return scoreDiff;
        const profitDiff =
          Number(b?.analysis?.metrics?.profit_cagr_5y || 0) - Number(a?.analysis?.metrics?.profit_cagr_5y || 0);
        if (profitDiff !== 0) return profitDiff;
        return String(a?.symbol || "").localeCompare(String(b?.symbol || ""));
      });

    return response(res, 200, responseUtils.SUCCESS, {
      limit,
      total: ranked.length,
      rows: ranked,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getGrowthAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const row = await growthAnalysisService.getGrowthAnalysisBySymbol(symbol);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, {
      ...row,
      analysis: growthAnalysisService.scoreGrowthCandidate(row),
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getGarpAnalysis = async (req, res) => {
  try {
    const asOfDate = getAsOfDateFromRequest(req);
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore = req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
      ? Number(req.query?.minScore)
      : null;
    const buckets = await garpAnalysisService.getGarpAnalysisBuckets({ limit, grade, minScore, asOfDate });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      filters: {
        limit,
        grade: grade || "ALL",
        minScore,
        asOfDate,
      },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getGarpAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const row = await garpAnalysisService.getGarpAnalysisBySymbol(symbol, asOfDate);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, {
      ...row,
      as_of_date: asOfDate,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getValueAnalysis = async (req, res) => {
  try {
    const asOfDate = getAsOfDateFromRequest(req);
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore = req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
      ? Number(req.query?.minScore)
      : null;
    const buckets = await valueAnalysisService.getValueAnalysisBuckets({ limit, grade, minScore, asOfDate });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      filters: { limit, grade: grade || "ALL", minScore, asOfDate },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getValueAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const row = await valueAnalysisService.getValueAnalysisBySymbol(symbol, asOfDate);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, { ...row, as_of_date: asOfDate });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getPivotAnalysis = async (req, res) => {
  try {
    const asOfDate = getAsOfDateFromRequest(req);
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore =
      req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
        ? Number(req.query?.minScore)
        : null;
    const includeRejected = String(req.query?.includeRejected || "false").toLowerCase() === "true";
    const buckets = await pivotAnalysisService.getPivotAnalysisBuckets({ limit, grade, minScore, includeRejected, asOfDate });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      summary: buckets.summary,
      filters: { limit, grade: grade || "ALL", minScore, includeRejected, asOfDate },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getPivotAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const asOfDate = getAsOfDateFromRequest(req);
    const row = await pivotAnalysisService.getPivotAnalysisBySymbol(symbol, asOfDate);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, { ...row, as_of_date: asOfDate });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getStockSearchSuggestions = async (req, res) => {
  try {
    const query = String(req.query?.q || "").trim();
    const suggestions = stockSearchService.suggestSearchFields(query);
    return response(res, 200, responseUtils.SUCCESS, {
      query,
      suggestions,
      total: suggestions.length,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.searchStocks = async (req, res) => {
  try {
    const query = String(req.query?.q || "").trim();
    const limit = Math.max(1, Math.min(100, Number(req.query?.limit || 50)));
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await stockSearchService.searchStocks({ query, limit, asOfDate });
    return response(res, 200, responseUtils.SUCCESS, {
      ...result,
      filters: {
        query,
        limit,
        asOfDate,
      },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.enqueueFundamentalsJob = async (req, res) => {
  try {
    const master_id = req.body?.master_id || "";
    const name = req.body?.name || "";
    if (!master_id && !name) {
      return response(res, 400, responseUtils.MASTER_ID_REQUIRED);
    }

    const masterStock = master_id
      ? await stockMasterService.getMasterStockById(master_id)
      : await stockMasterService.getMasterStockByName(name);

    if (!masterStock) return response(res, 400, responseUtils.STOCK_NOT_FOUND);
    if (!masterStock.is_active) {
      return response(res, 400, responseUtils.STOCK_INACTIVE);
    }
    if (String(masterStock.screener_status || "PENDING").toUpperCase() !== "PENDING") {
      return response(res, 400, responseUtils.SCREENER_STATUS_NOT_PENDING);
    }
    if (!masterStock.screener_url) {
      await stockMasterService.updateMasterStock(masterStock.id, {
      screener_status: "FAILED_NO_RETRY",
      }).catch(() => {});
      return response(res, 400, responseUtils.SCREENER_URL_NOT_EXIST);
    }

    const active = await activeStockService.getActiveStockByMasterId(masterStock.id);

    const payload = JSON.stringify({
      master_id: String(masterStock.id),
      active_stock_id: active?.id ? String(active.id) : null,
      name: masterStock.name || null,
      symbol: masterStock.symbol,
      screener_url: masterStock.screener_url,
    });

    const added = await redis.sadd(REDIS_KEYS.FUNDAMENTALS_DEDUPE, payload);
    if (added) {
      await redis.lpush(REDIS_KEYS.FUNDAMENTALS_QUEUE, payload);
      return response(res, 200, responseUtils.SUCCESS, { queued: true });
    }

    return response(res, 200, responseUtils.SUCCESS, {
      queued: false,
      message: "Already queued",
    });
  } catch (error) {
    return response(
      res,
      500,
      responseUtils.FAILED_TO_UPDATE_STOCK_FUNDAMENTALS,
      error,
    );
  }
};

exports.clearFundamentalsQueue = async (req, res) => {
  try {
    await redis.del(REDIS_KEYS.FUNDAMENTALS_QUEUE);
    await redis.del(REDIS_KEYS.FUNDAMENTALS_DEDUPE);
    return response(res, 200, responseUtils.SUCCESS, { cleared: true });
  } catch (error) {
    return response(
      res,
      500,
      responseUtils.FAILED_TO_UPDATE_STOCK_FUNDAMENTALS,
      error,
    );
  }
};

exports.enqueueAllFundamentalsJobs = async (req, res) => {
  try {
    await enqueueFundamentalsJobs();
    return response(res, 200, responseUtils.SUCCESS, { queued: true });
  } catch (error) {
    return response(
      res,
      500,
      responseUtils.FAILED_TO_UPDATE_STOCK_FUNDAMENTALS,
      error,
    );
  }
};

exports.previewFundamentalsByName = async (req, res) => {
  try {
    const name = req.body?.name || "";
    if (!name) return response(res, 400, responseUtils.MASTER_ID_REQUIRED);

    const masterStock = await stockMasterService.getMasterStockByName(name);
    if (!masterStock) return response(res, 400, responseUtils.STOCK_NOT_FOUND);
    if (!masterStock.is_active) {
      return response(res, 400, responseUtils.STOCK_INACTIVE);
    }
    if (String(masterStock.screener_status || "PENDING").toUpperCase() !== "PENDING") {
      return response(res, 400, responseUtils.SCREENER_STATUS_NOT_PENDING);
    }
    if (!masterStock.screener_url) {
      await stockMasterService.updateMasterStock(masterStock.id, {
      screener_status: "FAILED_NO_RETRY",
      }).catch(() => {});
      return response(res, 400, responseUtils.SCREENER_URL_NOT_EXIST);
    }

    const result = await scrapeWithFallback(masterStock.screener_url);
    const data = result.data;
    await writeFundamentalsTableRowCatalog(masterStock, data);
    await upsertMappedFundamentals(masterStock, data);
    if (result.fallbackUsed && result.selectedUrl && result.selectedUrl !== masterStock.screener_url) {
      await stockMasterService.updateMasterStock(masterStock.id, {
        screener_url: result.selectedUrl,
      });
    }
    await stockMasterService.updateMasterStock(masterStock.id, {
      screener_status: "VALID",
    }).catch(() => {});

    return response(res, 200, responseUtils.SUCCESS, { data });
  } catch (error) {
    if (req.body?.name) {
      const masterStock = await stockMasterService.getMasterStockByName(req.body.name).catch(() => null);
        if (masterStock?.id) {
          await stockMasterService.updateMasterStock(masterStock.id, {
          screener_status: "FAILED_NO_RETRY",
          }).catch(() => {});
        }
    }
    return response(
      res,
      500,
      responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS,
      { error: error?.message || error, stack: error?.stack },
    );
  }
};
