const { analyzeScreenerHtmlRendered } = require("../utils/html_extractor");

const analyzeScreenerHtmlRenderedService = async (url) => {
  if (!url) throw new Error("No URL provided");
  return analyzeScreenerHtmlRendered(url);
};

module.exports = {
  analyzeScreenerHtmlRendered: analyzeScreenerHtmlRenderedService,
};
