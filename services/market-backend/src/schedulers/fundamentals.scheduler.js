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
  const refreshBefore = new Date(
    Date.now() - DEFAULT_REFRESH_DAYS * 24 * 60 * 60 * 1000,
  );

  const fundamentals = await stockFundamentalsService.listMasterFreshness();
  const fundamentalsByMaster = new Map(
    fundamentals.map((f) => [String(f.master_id), f.last_updated_at]),
  );

  const masters = await stockMasterService.getAllMasterStocks();
  const candidateMasters = masters.filter((m) => m.screener_url);

  const activeStocks = await activeStockService.getActiveStocksByMasterIds(
    candidateMasters.map((m) => m.id),
  );

  const activeByMaster = new Map(
    activeStocks.map((a) => [String(a.master_id), String(a.id)]),
  );

  let queued = 0;
  for (const m of candidateMasters) {
    if (queued >= DEFAULT_BATCH) break;

    const lastUpdated = fundamentalsByMaster.get(String(m.id));
    const neverFetched = !m.fetch_count || m.fetch_count <= 0;
    const missing = !lastUpdated;
    const stale = lastUpdated && new Date(lastUpdated) < refreshBefore;
    if (!neverFetched && !missing && !stale) continue;

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
