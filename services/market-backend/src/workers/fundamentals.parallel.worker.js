require("dotenv").config();
require("../config/db");

const stockFundamentalsService = require("../services/stockFundamental.service");
const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const { launchChromium } = require("../utils/browserLauncher");
const { scrapeWithFallback } = require("../services/fundamentalsScrape.service");
const { buildMappedFundamentals } = require("../services/fundamentalsMapper.service");

const FAST_CONCURRENCY = Math.max(
  1,
  Number(process.env.FUNDAMENTALS_PARALLELISM || 2),
);
const IDLE_DELAY_MS = Number(process.env.FUNDAMENTALS_IDLE_DELAY_MS || 10000);
const REFRESH_DAYS = Number(process.env.FUNDAMENTALS_REFRESH_DAYS || 30);
const CANDIDATE_LIMIT = Number(process.env.FUNDAMENTALS_QUEUE_BATCH || 200);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const argv = process.argv.slice(2);
const readArg = (...names) => {
  for (const name of names) {
    const prefix = `--${name}=`;
    const hit = argv.find((arg) => arg.startsWith(prefix));
    if (hit) return hit.slice(prefix.length);
  }
  return null;
};
const hasFlag = (...names) =>
  names.some(
    (name) => argv.includes(`--${name}`) || argv.includes(`--${name}=true`),
  );

const runtime = {
  masterId: readArg("master-id", "stock-id", "id"),
  token: readArg("token"),
  symbol: readArg("symbol"),
  mode: (readArg("mode") || "pending").toLowerCase(),
  once: hasFlag("once", "single", "test"),
  concurrency: Math.max(1, Number(readArg("concurrency") || FAST_CONCURRENCY)),
  waitUntil: (readArg("wait-until") || "domcontentloaded").toLowerCase(),
};

runtime.singleMode = Boolean(
  runtime.masterId || runtime.token || runtime.symbol || runtime.once,
);

const getActiveStockId = async (masterId) => {
  if (!masterId) return null;
  const active = await activeStockService.getActiveStockByMasterId(masterId);
  return active?.id || null;
};

const hasUsableFundamentals = (data) => {
  if (!data || typeof data !== "object") return false;
  if (data?.company_info?.company_name) return true;

  const mapped = buildMappedFundamentals(data);
  const sections = [
    mapped?.peers?.main_table?.rows,
    mapped?.tables?.quarters?.rows,
    mapped?.tables?.profit_loss?.rows,
    mapped?.tables?.balance_sheet?.rows,
    mapped?.tables?.cash_flow?.rows,
    mapped?.tables?.ratios?.rows,
    mapped?.tables?.shareholdings?.rows,
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

const updateMasterFundamentalsStatus = async (masterId, payload = {}) => {
  if (!masterId) return null;
  const updatePayload = {
    fundamentals_status: payload.fundamentals_status || "PENDING",
    fundamentals_checked_at: payload.fundamentals_checked_at || new Date(),
    fundamentals_failed_fields: payload.fundamentals_failed_fields || null,
    fundamentals_failed_reason: payload.fundamentals_failed_reason || null,
  };
  if (payload.screener_status) {
    updatePayload.screener_status = payload.screener_status;
  }
  try {
    return await stockMasterService.updateMasterStock(masterId, updatePayload);
  } catch (err) {
    return null;
  }
};

const fetchCandidatesFromDb = async (_mode = "pending", limit = CANDIDATE_LIMIT) => {
  const masters = await stockMasterService.getAllMasterStocks();
  const candidates = masters.filter((m) => stockMasterService.canFetchScreener(m));

  const activeStocks = await activeStockService.getActiveStocksByMasterIds(
    candidates.map((m) => m.id),
  );
  const activeByMaster = new Map(
    activeStocks.map((a) => [String(a.master_id), String(a.id)]),
  );

  const out = [];
  for (const m of candidates) {
    if (out.length >= limit) break;

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

const fetchSingleCandidate = async () => {
  let master = null;

  if (runtime.masterId) {
    master = await stockMasterService.getMasterStockById(runtime.masterId);
  } else if (runtime.token) {
    master = await stockMasterService.getMasterStockByToken(runtime.token);
  } else if (runtime.symbol) {
    master = await stockMasterService.getMasterStockBySymbol(runtime.symbol);
  }

  if (!master) {
    throw new Error(
      `Test stock not found for ${runtime.masterId ? `master_id=${runtime.masterId}` : runtime.token ? `token=${runtime.token}` : `symbol=${runtime.symbol}`}`,
    );
  }

  if (!stockMasterService.canFetchScreener(master)) {
    if (!master?.is_active) {
      throw new Error("Inactive stock cannot be fetched");
    }
    if (String(master?.screener_status || "PENDING").toUpperCase() !== "PENDING") {
      throw new Error(`Screener status is ${String(master?.screener_status || "PENDING").toUpperCase()}`);
    }
    if (!master?.screener_url) {
      throw new Error("Missing screener_url");
    }
    throw new Error("Stock is not eligible for screener fetch");
  }

  if (!master.screener_url) {
    throw new Error("Missing screener_url");
  }

  const active = await activeStockService.getActiveStockByMasterId(master.id);
  return [
    {
      master_id: String(master.id),
      active_stock_id: active?.id ? String(active.id) : null,
      name: master.name || null,
      symbol: master.symbol || null,
      screener_url: master.screener_url || null,
    },
  ];
};

const processCandidate = async (payload, browser, position, total, stats) => {
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
    `[fundamentals-fast] In progress ${position}/${total}: ${label} | mode=${runtime.mode} | browser=${browser?._guid ? "pooled" : "shared"}`,
  );

  try {
    const result = await scrapeWithFallback(screenerUrl, {
      browser,
      waitUntil: runtime.waitUntil,
    });

    const data = result.data;
    if (!hasUsableFundamentals(data)) {
      throw new Error("Empty/invalid fundamentals extracted from screener");
    }

    await upsertFundamentals(payload, data);

    if (masterId && result.fallbackUsed && result.selectedUrl && result.selectedUrl !== screenerUrl) {
      await stockMasterService.updateMasterStock(masterId, {
        screener_url: result.selectedUrl,
      });
      console.log(
        `[fundamentals-fast] Updated screener_url for ${label} => ${result.selectedUrl}`,
      );
    }

    await updateMasterFundamentalsStatus(masterId, {
      fundamentals_status: "VALID",
      fundamentals_checked_at: new Date(),
      fundamentals_failed_fields: [],
      fundamentals_failed_reason: null,
      screener_status: "VALID",
    });

    if (stats) stats.done += 1;
    return { ok: true, label };
  } catch (err) {
    const failedFields = Array.isArray(err?.failedFields) ? err.failedFields : null;
    await updateMasterFundamentalsStatus(masterId, {
      fundamentals_status: "FAILED",
      fundamentals_checked_at: new Date(),
      fundamentals_failed_fields: failedFields,
      fundamentals_failed_reason: err?.message || String(err),
      screener_status: "FAILED",
    });
    if (stats) stats.failed += 1;
    console.error(
      `[fundamentals-fast] Failed ${position}/${total}: ${label} | done=${stats?.done || 0}, failed=${stats?.failed || 0} | error=${
        err?.message || err
      }`,
    );
    return { ok: false, label, error: err };
  }
};

const createBrowserPool = async (size) => {
  const browsers = [];
  for (let i = 0; i < size; i += 1) {
    browsers.push(await launchChromium());
  }
  return browsers;
};

const runBatchOnce = async () => {
  const stats = { done: 0, failed: 0 };
  const candidates = runtime.singleMode
    ? await fetchSingleCandidate()
    : await fetchCandidatesFromDb(runtime.mode, CANDIDATE_LIMIT);

  if (!candidates.length) {
    if (runtime.singleMode) {
      console.log("[fundamentals-fast] No matching stock found for single-run mode");
      return false;
    }
    return false;
  }

  const browsers = await createBrowserPool(
    Math.min(runtime.concurrency, Math.max(1, candidates.length)),
  );

  console.log(
    `[fundamentals-fast] Candidates=${candidates.length}, browsers=${browsers.length}, mode=${runtime.mode}`,
  );

  let nextIndex = 0;
  const workers = browsers.map(async (browser) => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= candidates.length) break;
      const payload = candidates[currentIndex];
      await processCandidate(
        payload,
        browser,
        currentIndex + 1,
        candidates.length,
        stats,
      );
    }
  });

  try {
    await Promise.all(workers);
  } finally {
    await Promise.all(
      browsers.map((browser) => browser.close().catch(() => {})),
    );
  }

  console.log(
    `[fundamentals-fast] Batch complete | done=${stats.done}, failed=${stats.failed}`,
  );
  return true;
};

const runWorker = async () => {
  console.log("Fundamentals fast worker started (parallel browser mode)");

  while (true) {
    const didWork = await runBatchOnce();
    if (runtime.singleMode) {
      return;
    }

    if (!didWork) {
      await sleep(IDLE_DELAY_MS);
      continue;
    }
  }
};

runWorker().catch((err) => {
  console.error("Fundamentals fast worker crashed", err);
  process.exit(1);
});
