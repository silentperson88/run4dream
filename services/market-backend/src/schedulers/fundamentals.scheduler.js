const cron = require("node-cron");
const redis = require("../config/redis.config");
const stockMasterService = require("../services/stockMaster.service");
const activeStockService = require("../services/activestock.service");
const stockFundamentalsService = require("../services/stockFundamental.service");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");

const DEFAULT_REFRESH_DAYS = parseInt(
  process.env.FUNDAMENTALS_REFRESH_DAYS || "30",
  10,
);

const DEFAULT_BATCH = parseInt(
  process.env.FUNDAMENTALS_QUEUE_BATCH || "200",
  10,
);

async function enqueueFundamentalsJobs() {
  const masters = await stockMasterService.getAllMasterStocks();
  const candidateMasters = masters.filter((m) => stockMasterService.canFetchScreener(m));

  const activeStocks = await activeStockService.getActiveStocksByMasterIds(
    candidateMasters.map((m) => m.id),
  );

  const activeByMaster = new Map(
    activeStocks.map((a) => [String(a.master_id), String(a.id)]),
  );

  let queued = 0;
  for (const m of candidateMasters) {
    if (queued >= DEFAULT_BATCH) break;

    const active_stock_id = activeByMaster.get(String(m.id)) || null;

    const payload = JSON.stringify({
      master_id: String(m.id),
      active_stock_id,
      name: m.name || null,
      symbol: m.symbol,
      screener_url: m.screener_url,
    });

    const added = await redis.sadd(REDIS_KEYS.FUNDAMENTALS_DEDUPE, payload);
    if (added) {
      await redis.lpush(REDIS_KEYS.FUNDAMENTALS_QUEUE, payload);
      queued++;
    }
  }

  if (queued) {
    console.log(`Enqueued ${queued} fundamentals jobs`);
  }
}

function startFundamentalsScheduler() {
  enqueueFundamentalsJobs().catch((e) =>
    console.error("Fundamentals enqueue failed", e),
  );

  cron.schedule("30 2 * * *", () => {
    enqueueFundamentalsJobs().catch((e) =>
      console.error("Fundamentals enqueue failed", e),
    );
  });
}

module.exports = { startFundamentalsScheduler, enqueueFundamentalsJobs };
