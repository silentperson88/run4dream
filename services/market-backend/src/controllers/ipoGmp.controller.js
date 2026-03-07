const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");
const ipoGmpService = require("../services/ipoGmp.service");

exports.fetchLiveIpoGmp = async (req, res) => {
  try {
    const result = await ipoGmpService.fetchAndStoreLiveIpoGmp();
    return response(res, 200, responseUtils.SUCCESS, result);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      error: error?.message || String(error),
    });
  }
};

exports.getLiveIpoGmpPaginated = async (req, res) => {
  try {
    const { page, limit } = req.query || {};
    const result = await ipoGmpService.getLiveIpoGmpPaginated({ page, limit });
    return response(res, 200, responseUtils.SUCCESS, result);
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      error: error?.message || String(error),
    });
  }
};
