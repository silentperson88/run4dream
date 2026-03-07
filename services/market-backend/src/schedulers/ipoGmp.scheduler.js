const cron = require("node-cron");
const ipoGmpService = require("../services/ipoGmp.service");

const runIpoGmpSync = async () => {
  try {
    const result = await ipoGmpService.fetchAndStoreLiveIpoGmp();
    console.log(
      `IPO GMP sync complete: scraped=${result.total_scraped}, inserted=${result.inserted}, updated=${result.updated}`,
    );
  } catch (error) {
    console.error("IPO GMP sync failed", error?.message || error);
  }
};

const startIpoGmpScheduler = () => {
  runIpoGmpSync();
  cron.schedule("0 * * * *", () => {
    runIpoGmpSync();
  });
};

module.exports = {
  startIpoGmpScheduler,
  runIpoGmpSync,
};
