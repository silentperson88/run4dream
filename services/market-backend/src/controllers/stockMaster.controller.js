const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const stockFundamentalsService = require("../services/stockFundamental.service");
const rawStockService = require("../services/rawstock.service");
const { withTransaction } = require("../repositories/tx");

const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");

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
        token: rawStock.token,
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

exports.syncCompanyFromFundamental = async (req, res) => {
  try {
    const result = await stockMasterService.syncCompanyFromFundamental();
    return res.json({
      message: "Company sync completed",
      modified: result.modified,
    });
  } catch (err) {
    console.error("Company sync failed:", err);
    return res.status(500).json({ error: err.message });
  }
};
