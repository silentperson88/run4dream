const { analyzeScreenerHtmlRendered } = require("../utils/html_extractor");

const analyzeScreenerHtmlRenderedService = async (url, options = {}) => {
  if (!url) throw new Error("No URL provided");
  return analyzeScreenerHtmlRendered(url, options);
};

module.exports = {
  analyzeScreenerHtmlRendered: analyzeScreenerHtmlRenderedService,
};
