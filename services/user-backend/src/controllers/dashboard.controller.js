const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");
const { getDashboardData } = require("../services/dashboard.service");

async function getDashboard(req, res) {
  try {
    const days = Number(req.query.days || 30);
    const data = await getDashboardData({
      user_id: req.user.id,
      days,
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

module.exports = { getDashboard };
