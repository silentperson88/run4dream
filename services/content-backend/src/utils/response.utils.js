const { MESSAGES } = require("./constants/response.constants");

/**
 * Response
 *
 * @param {*} res
 * @param {*} status
 * @param {*} msg
 * @param {} data
 * @returns
 */
exports.response = (
  res,
  status,
  msg = MESSAGES.COMMON.SOMETHING_WENT_WRONG,
  data = [],
) => {
  return res.status(status).json({
    message: msg ?? "Success",
    data: data,
  });
};
