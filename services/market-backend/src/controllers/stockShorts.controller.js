const { getStockShortsTopics } = require("../services/stockShorts.service");
const { getAsOfDateFromRequest } = require("../utils/asOfDate.utils");

async function getTopics(req, res) {
  try {
    const asOfDate = getAsOfDateFromRequest(req);
    const result = await getStockShortsTopics({ asOfDate });

    return res.json({
      success: true,
      message: "Stock shorts topics fetched successfully",
      data: result,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to fetch stock shorts topics",
    });
  }
}

module.exports = {
  getTopics,
};
