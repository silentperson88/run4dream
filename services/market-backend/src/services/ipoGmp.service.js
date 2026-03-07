const ipoGmpRepo = require("../repositories/ipoGmp.repository");
const {
  scrapeLiveIpoGmpRows,
  LIVE_IPO_GMP_URL,
} = require("./ipoGmpScraper.service");

const fetchAndStoreLiveIpoGmp = async () => {
  const scrapedRows = await scrapeLiveIpoGmpRows(LIVE_IPO_GMP_URL);
  const saved = await ipoGmpRepo.upsertLiveIpoRows(scrapedRows);
  return {
    source_url: LIVE_IPO_GMP_URL,
    total_scraped: scrapedRows.length,
    ...saved,
  };
};

const getLiveIpoGmpPaginated = async ({ page, limit } = {}) =>
  ipoGmpRepo.listLiveIpoRows({ page, limit });

module.exports = {
  fetchAndStoreLiveIpoGmp,
  getLiveIpoGmpPaginated,
};
