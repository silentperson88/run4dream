require("dotenv").config();
require("../config/db");

const stockFundamentalsService = require("../services/stockFundamental.service");
const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const { analyzeScreenerHtmlRendered } = require("../services/screenerHtmlRendered.service");
const { buildMappedFundamentals } = require("../services/fundamentalsMapper.service");

const JOB_DELAY_MS = Number(process.env.FUNDAMENTALS_JOB_DELAY_MS || 5000);
const IDLE_DELAY_MS = Number(process.env.FUNDAMENTALS_IDLE_DELAY_MS || 10000);
const REFRESH_DAYS = Number(process.env.FUNDAMENTALS_REFRESH_DAYS || 30);
const CANDIDATE_LIMIT = Number(process.env.FUNDAMENTALS_QUEUE_BATCH || 200);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getActiveStockId = async (masterId) => {
  if (!masterId) return null;
  const active = await activeStockService.getActiveStockByMasterId(masterId);
  return active?.id || null;
};

const hasUsableFundamentals = (data) => {
  if (!data || typeof data !== "object") return false;
  if (data?.company_info?.company_name) return true;

  const sections = [
    data?.peers?.main_table?.rows,
    data?.quarters?.main_table?.rows,
    data?.profit_loss?.main_table?.rows,
    data?.balance_sheet?.main_table?.rows,
    data?.cash_flow?.main_table?.rows,
    data?.ratios?.main_table?.rows,
    data?.shareholdings?.main_table?.rows,
  ];
  return sections.some((rows) => Array.isArray(rows) && rows.length > 0);
};

const upsertFundamentals = async (payload, data) => {
  const masterId = Number(payload.master_id);
  if (!masterId) return null;

  const existing = await stockFundamentalsService.getFullStockFundamentals(masterId);
  let activeStockId = existing?.active_stock_id || null;
  if (!activeStockId) {
    activeStockId = await getActiveStockId(masterId);
  }

  if (!activeStockId) {
    throw new Error("active_stock_id not found");
  }

  const mapped = buildMappedFundamentals(data);
  const updated = await stockFundamentalsService.upsertFundamentals({
    master_id: masterId,
    active_stock_id: activeStockId,
    company: mapped.company,
    company_info: mapped.company_info,
    summary: mapped.summary,
    peers: mapped.peers,
    tables: mapped.tables,
    other_details: mapped.other_details,
    documents: mapped.documents,
    raw_payload: mapped.raw_payload,
    last_updated_at: new Date(),
  });

  if (updated) {
    await stockMasterService.setFetchCount(masterId, 1);
  }
  return updated;
};

const fetchCandidatesFromDb = async (limit = CANDIDATE_LIMIT) => {
  const refreshBefore = new Date(
    Date.now() - REFRESH_DAYS * 24 * 60 * 60 * 1000,
  );

  const fundamentals = await stockFundamentalsService.listMasterFreshness();
  const fundamentalsByMaster = new Map(
    fundamentals.map((f) => [String(f.master_id), f.last_updated_at]),
  );

  const masters = await stockMasterService.getAllMasterStocks();
  const candidates = masters.filter((m) => m.screener_url);

  const activeStocks = await activeStockService.getActiveStocksByMasterIds(
    candidates.map((m) => m.id),
  );
  const activeByMaster = new Map(
    activeStocks.map((a) => [String(a.master_id), String(a.id)]),
  );

  const out = [];
  for (const m of candidates) {
    if (out.length >= limit) break;

    const lastUpdated = fundamentalsByMaster.get(String(m.id));
    const neverFetched = !m.fetch_count || m.fetch_count <= 0;
    const missing = !lastUpdated;
    const stale = lastUpdated && new Date(lastUpdated) < refreshBefore;
    if (!neverFetched && !missing && !stale) continue;

    out.push({
      master_id: String(m.id),
      active_stock_id: activeByMaster.get(String(m.id)) || null,
      name: m.name || null,
      symbol: m.symbol || null,
      screener_url: m.screener_url || null,
    });
  }

  return out;
};

const runWorker = async () => {
  console.log("Fundamentals worker started (DB candidate mode)");
  let doneCount = 0;
  let failedCount = 0;
  let totalStarted = 0;

  while (true) {
    const candidates = await fetchCandidatesFromDb();
    if (!candidates.length) {
      await sleep(IDLE_DELAY_MS);
      continue;
    }

    const runTotal = candidates.length;
    console.log(`[fundamentals] DB candidates fetched: total=${runTotal}`);

    for (let i = 0; i < candidates.length; i += 1) {
      const payload = candidates[i];
      totalStarted += 1;

      const masterId = Number(payload.master_id);
      const master =
        Number.isFinite(masterId) && masterId > 0
          ? await stockMasterService.getMasterStockById(masterId)
          : null;
      const label =
        master?.name ||
        payload?.name ||
        master?.symbol ||
        payload?.symbol ||
        `master_id=${payload?.master_id || "unknown"}`;
      const screenerUrl = master?.screener_url || payload?.screener_url || "";

      console.log(
        `[fundamentals] In progress ${i + 1}/${runTotal}: ${label} | done=${doneCount}, failed=${failedCount}`,
      );

      try {
        if (!screenerUrl) throw new Error("Missing screener_url");

        const data = await analyzeScreenerHtmlRendered(screenerUrl);
        if (!data) throw new Error("No data returned from screener parser");
        if (!hasUsableFundamentals(data)) {
          throw new Error("Empty/invalid fundamentals extracted from screener");
        }

        await upsertFundamentals(payload, data);
        doneCount += 1;
        console.log(
          `[fundamentals] Done ${i + 1}/${runTotal}: ${label} | done=${doneCount}, failed=${failedCount}`,
        );
      } catch (err) {
        failedCount += 1;
        console.error(
          `[fundamentals] Failed ${i + 1}/${runTotal}: ${label} | done=${doneCount}, failed=${failedCount} | error=${
            err?.message || err
          }`,
        );
      }

      if (JOB_DELAY_MS > 0 && i < candidates.length - 1) {
        await sleep(JOB_DELAY_MS);
      }
    }
  }
};

runWorker().catch((err) => {
  console.error("Fundamentals worker crashed", err);
  process.exit(1);
});
