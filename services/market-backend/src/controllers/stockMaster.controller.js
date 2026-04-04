const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const stockFundamentalsService = require("../services/stockFundamental.service");
const rawStockService = require("../services/rawstock.service");
const activeStocksRepo = require("../repositories/activeStocks.repository");
const { SmartApiPriceService } = require("../services/smartapi.service");
const { withTransaction } = require("../repositories/tx");

const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");

const smartApiPriceService = new SmartApiPriceService();

const pickNumber = (...values) => {
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return 0;
};

const mapFullPriceSnapshot = (item = {}) => ({
  ltp: pickNumber(item.ltp, item.close, item.open, item.high),
  open: pickNumber(item.open),
  high: pickNumber(item.high),
  low: pickNumber(item.low),
  close: pickNumber(item.close),
  percentChange: pickNumber(item.percentChange, item.change, item.netChange),
  avgPrice: pickNumber(item.avgPrice, item.averagePrice),
  lowerCircuit: pickNumber(item.lowerCircuit),
  upperCircuit: pickNumber(item.upperCircuit),
  week52Low: pickNumber(item.week52Low, item["52WeekLow"]),
  week52High: pickNumber(item.week52High, item["52WeekHigh"]),
});

exports.createrMasterStock = async (req, res) => {
  try {
    const { rawStockId, status, screenerUrl } = req.body;

    if (!rawStockId || !status) {
      return response(res, 400, responseUtils.RAW_STOCK_ID_AND_STATUS_REQUIRED);
    }

    const allowedStatus = ["approved", "rejected"];
    if (!allowedStatus.includes(status)) {
      return response(res, 400, responseUtils.INVALID_STATUS_VALUE);
    }

    const result = await withTransaction(async (client) => {
      const rawStock = await rawStockService.updateRawStock(
        rawStockId,
        { status },
        client,
      );

      if (!rawStock) {
        throw new Error(responseUtils.RAW_STOCK_NOT_FOUND);
      }

      if (status === "rejected") {
        return { status: "rejected" };
      }

      const body = {
        token: rawStock.token || null,
        symbol: rawStock.symbol,
        name: rawStock.name,
        exchange: rawStock.exch_seg || rawStock.exchange,
        instrumenttype: rawStock.instrumenttype,
        lotsize: rawStock.lotsize,
        tick_size: rawStock.tick_size,
        raw_stock_id: rawStock.id,
        screener_status: "PENDING",
        screener_url:
          screenerUrl ||
          `https://www.screener.in/company/${rawStock.name}/consolidated/`,
        security_code: rawStock.security_code,
        history_range: null,
      };

      const masterStock = await stockMasterService.createMasterStock(body, client);

      const activeStock = await activeStockService.addStock(
        {
          ...body,
          master_id: masterStock.id,
        },
        client,
      );

      await stockFundamentalsService.createEntry(
        masterStock.id,
        activeStock.id,
        client,
      );

      return { status: "approved", masterStock };
    });

    if (result.status === "rejected") {
      return response(res, 200, responseUtils.RAW_STOCK_REJECTED_SUCCESSFULLY);
    }

    return response(
      res,
      201,
      responseUtils.MASTER_CRATED_SUCCESSFULLY,
      result.masterStock,
    );
  } catch (err) {
    if (err.code === "23505" || err.message === responseUtils.STOCK_ALREADY_EXISTS) {
      return response(res, 409, responseUtils.STOCK_ALREADY_EXISTS, err);
    }

    return response(res, 500, responseUtils.ERROR, {
      message: err.message,
    });
  }
};

exports.getMasterList = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 50;
    const search = req.query.search || "";

    const query = {
      search,
      is_active: true,
    };

    const result = await stockMasterService.getMasterList(page, limit, query);

    return response(
      res,
      200,
      responseUtils.MASTER_FETCHED_SUCCESSFULLY,
      result,
    );
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.getMasterStockByToken = async (req, res) => {
  try {
    const stock = await stockMasterService.getMasterStockByToken(
      req.params.token,
    );
    return response(res, 200, responseUtils.MASTER_FETCHED_SUCCESSFULLY, stock);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.updateTokenAndExchange = async (req, res) => {
  try {
    const masterId = Number(req.params.id);
    const { token, exchange } = req.body || {};

    if (!Number.isFinite(masterId) || masterId <= 0) {
      return response(res, 400, responseUtils.INVALID_STATUS_VALUE, {
        message: "Valid master id is required",
      });
    }

    const normalizedExchange = String(exchange || "").trim().toUpperCase();
    if (!normalizedExchange || !["NSE", "BSE"].includes(normalizedExchange)) {
      return response(res, 400, responseUtils.INVALID_STATUS_VALUE, {
        message: "Valid exchange is required",
      });
    }

    const normalizedToken = String(token || "").trim() || null;
    if (!normalizedToken) {
      return response(res, 400, responseUtils.INVALID_STATUS_VALUE, {
        message: "Token is required",
      });
    }

    const masterStock = await stockMasterService.updateMasterStock(masterId, {
      token: normalizedToken,
      exchange: normalizedExchange,
    });

    if (!masterStock) {
      return response(res, 404, responseUtils.ACTIVE_STOCK_NOT_FOUND, {
        message: "Master stock not found",
      });
    }

    const priceResponse = await smartApiPriceService.getMarketData(
      "FULL",
      [normalizedToken],
      normalizedExchange,
    );
    const fetched = Array.isArray(priceResponse?.data?.data?.fetched)
      ? priceResponse.data.data.fetched[0]
      : Array.isArray(priceResponse?.data?.fetched)
        ? priceResponse.data.fetched[0]
        : null;

    const priceData = fetched ? mapFullPriceSnapshot(fetched) : {};
    const activeStock = await activeStocksRepo.updateByMasterId(masterId, {
      token: normalizedToken,
      exchange: normalizedExchange,
      ...priceData,
    }).catch(() => null);

    return response(res, 200, responseUtils.MASTER_FETCHED_SUCCESSFULLY, {
      masterStock,
      activeStock,
      fetchedPrice: fetched || null,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.markMasterInactive = async (req, res) => {
  try {
    const masterId = Number(req.params.id);
    if (!Number.isFinite(masterId) || masterId <= 0) {
      return response(res, 400, responseUtils.INVALID_STATUS_VALUE, {
        message: "Valid master id is required",
      });
    }

    const masterStock = await stockMasterService.updateMasterStock(masterId, {
      is_active: false,
    });

    if (!masterStock) {
      return response(res, 404, responseUtils.ACTIVE_STOCK_NOT_FOUND, {
        message: "Master stock not found",
      });
    }

    return response(res, 200, responseUtils.MASTER_FETCHED_SUCCESSFULLY, {
      masterStock,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error.message,
    });
  }
};

exports.syncCompanyFromFundamental = async (req, res) =>
  res.status(410).json({
    success: false,
    message: "Company sync removed; stock_master no longer stores company.",
  });
