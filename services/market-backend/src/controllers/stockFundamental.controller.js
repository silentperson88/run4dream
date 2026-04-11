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
const { pythonApi } = require("../pythonApi/apiService.py");
const { PYTHON_ENDPOINTS } = require("../pythonApi/endpoints.py");
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");
const { enqueueFundamentalsJobs } = require("../schedulers/fundamentals.scheduler");
const { scrapeWithFallback } = require("../services/fundamentalsScrape.service");
const { buildMappedFundamentals } = require("../services/fundamentalsMapper.service");
const stockTechnicalService = require("../services/stockTechnical.service");

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

const buildOverviewFundamentalsPayload = async (masterStock) => {
  const overviewRow = await stockFundamentalsService.getOverviewFundamentalsByMasterId(masterStock.id);
  if (!overviewRow) return null;

  const technicals = await stockTechnicalService.getMomentumSnapshotByMasterId(masterStock.id).catch(() => null);

  return {
    master_id: String(masterStock.id),
    active_stock_id: overviewRow.active_stock_id ? String(overviewRow.active_stock_id) : null,
    company: overviewRow.company_name || masterStock.name || null,
    company_info: {
      company_name: overviewRow.company_name || masterStock.name || null,
      about: overviewRow.about || null,
      key_points: overviewRow.key_points || null,
      links: Array.isArray(overviewRow.links) ? overviewRow.links : [],
    },
    summary: {
      market_snapshot: {
        market_cap: overviewRow.market_cap,
        current_price: overviewRow.current_price,
        high_low: overviewRow.high_low,
        stock_pe: overviewRow.stock_pe,
        pe_ratio: overviewRow.stock_pe,
        book_value: overviewRow.book_value,
        dividend_yield: overviewRow.dividend_yield,
        roce: overviewRow.roce,
        roe: overviewRow.roe,
        face_value: overviewRow.face_value,
      },
      pros: Array.isArray(overviewRow.pros) ? overviewRow.pros : [],
      cons: Array.isArray(overviewRow.cons) ? overviewRow.cons : [],
    },
    documents: {},
    technicals,
    last_updated_at: overviewRow.last_updated_at || masterStock.updated_at || new Date().toISOString(),
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
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const [fundamentalsResult, momentumResult] = await Promise.allSettled([
      stockFundamentalsService.getFullStockFundamentals(master.id),
      stockTechnicalService.getMomentumSnapshotByMasterId(master.id),
    ]);

    const data = fundamentalsResult.status === "fulfilled" ? fundamentalsResult.value : null;
    if (!data) {
      return response(res, 400, responseUtils.FAILED_TO_FETCH_STOCK_FUNDAMENTALS);
    }

    return response(res, 200, responseUtils.SUCCESS, {
      ...data,
      technicals: momentumResult.status === "fulfilled" ? momentumResult.value : null,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getOverviewStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const data = await buildOverviewFundamentalsPayload(master);
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
    const master = await stockMasterService.getMasterStockBySymbol(symbol);
    if (!master) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    const rows = await stockFundamentalsService.getQuarterlyFundamentalsBySymbol(symbol);
    return response(res, 200, responseUtils.SUCCESS, {
      symbol: master.symbol || symbol,
      company_name: master.name || null,
      master_id: String(master.id),
      active_stock_id: rows?.[0]?.active_stock_id ? String(rows[0].active_stock_id) : null,
      rows: Array.isArray(rows) ? rows : [],
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

const buildSplitSectionResponse = async (symbol, section) => {
  const master = await stockMasterService.getMasterStockBySymbol(symbol);
  if (!master) return { status: 400, body: responseUtils.STOCK_NOT_FOUND };

  const getterMap = {
    profit_loss: stockFundamentalsService.getProfitLossFundamentalsBySymbol,
    balance_sheet: stockFundamentalsService.getBalanceSheetFundamentalsBySymbol,
    cash_flow: stockFundamentalsService.getCashFlowFundamentalsBySymbol,
    ratios: stockFundamentalsService.getRatiosFundamentalsBySymbol,
    shareholdings: stockFundamentalsService.getShareholdingFundamentalsBySymbol,
  };

  const rows = await getterMap[section](symbol);
  return {
    status: 200,
    body: responseUtils.SUCCESS,
    data: {
      symbol: master.symbol || symbol,
      company_name: master.name || null,
      master_id: String(master.id),
      active_stock_id: rows?.[0]?.active_stock_id ? String(rows[0].active_stock_id) : null,
      rows: Array.isArray(rows) ? rows : [],
    },
  };
};

exports.getProfitLossStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const result = await buildSplitSectionResponse(symbol, "profit_loss");
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getBalanceSheetStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const result = await buildSplitSectionResponse(symbol, "balance_sheet");
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getCashFlowStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const result = await buildSplitSectionResponse(symbol, "cash_flow");
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getRatiosStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const result = await buildSplitSectionResponse(symbol, "ratios");
    if (result.status !== 200) return response(res, result.status, result.body);
    return response(res, 200, result.body, result.data);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getShareholdingStockFundamentalsBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const result = await buildSplitSectionResponse(symbol, "shareholdings");
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
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore = req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
      ? Number(req.query?.minScore)
      : null;
    const buckets = await garpAnalysisService.getGarpAnalysisBuckets({ limit, grade, minScore });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      filters: {
        limit,
        grade: grade || "ALL",
        minScore,
      },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getGarpAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const row = await garpAnalysisService.getGarpAnalysisBySymbol(symbol);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, {
      ...row,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getValueAnalysis = async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore = req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
      ? Number(req.query?.minScore)
      : null;
    const buckets = await valueAnalysisService.getValueAnalysisBuckets({ limit, grade, minScore });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      filters: { limit, grade: grade || "ALL", minScore },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getValueAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const row = await valueAnalysisService.getValueAnalysisBySymbol(symbol);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, { ...row });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getPivotAnalysis = async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
    const grade = String(req.query?.grade || "ALL");
    const minScore =
      req.query?.minScore !== undefined && req.query?.minScore !== null && req.query?.minScore !== ""
        ? Number(req.query?.minScore)
        : null;
    const includeRejected = String(req.query?.includeRejected || "false").toLowerCase() === "true";
    const buckets = await pivotAnalysisService.getPivotAnalysisBuckets({ limit, grade, minScore, includeRejected });
    return response(res, 200, responseUtils.SUCCESS, {
      total: buckets.total,
      rows: buckets.overallRows,
      buckets: buckets.tierRows,
      summary: buckets.summary,
      filters: { limit, grade: grade || "ALL", minScore, includeRejected },
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, error);
  }
};

exports.getPivotAnalysisBySymbol = async (req, res) => {
  try {
    const { symbol } = req.params;
    const row = await pivotAnalysisService.getPivotAnalysisBySymbol(symbol);
    if (!row) return response(res, 400, responseUtils.STOCK_NOT_FOUND);

    return response(res, 200, responseUtils.SUCCESS, { ...row });
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
    const result = await stockSearchService.searchStocks({ query, limit });
    return response(res, 200, responseUtils.SUCCESS, {
      ...result,
      filters: {
        query,
        limit,
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
