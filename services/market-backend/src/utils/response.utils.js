const constantUtils = require("../utils/constants.utils");

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
  msg = constantUtils.SOMETHING_WENT_WRONG,
  data = []
) => {
  // if (!res) return { status, msg, data };

  return res.status(status).json({
    message: msg ?? "Success",
    data: data,
  });
};
