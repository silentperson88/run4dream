const { scrapeGoogleAiAnswer } = require("../services/googleAiScrape.service");

const googleAiScrapeAnswer = async (req, res) => {
  try {
    const question = String(req.body?.question || "").trim();
    if (!question) {
      return res.status(400).json({
        success: false,
        message: "question is required",
      });
    }

    const result = await scrapeGoogleAiAnswer({ question });

    return res.status(200).json({
      success: true,
      data: result,
    });
  } catch (error) {
    return res.status(400).json({
      success: false,
      message: error.message || "Failed to scrape Google AI answer",
    });
  }
};

module.exports = {
  googleAiScrapeAnswer,
};
