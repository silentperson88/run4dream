const { listPortfolioTypes } = require("../services/portfolioType.service");
const { MESSAGES } = require("../utils/constants/response.constants");
const { response } = require("../utils/response.utils");

exports.getPortFoliosTypeList = async (req, res) => {
  try {
    const result = await listPortfolioTypes();
    return response(res, 200, MESSAGES.COMMON.SUCCESS, result);
  } catch (error) {
    return response(res, 400, error.message);
  }
};
