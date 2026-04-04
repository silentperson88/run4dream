require("dotenv").config();
require("../config/db");

const stockFundamentalsService = require("../services/stockFundamental.service");
const activeStockService = require("../services/activestock.service");
const stockMasterService = require("../services/stockMaster.service");
const { scrapeWithFallback } = require("../services/fundamentalsScrape.service");
const { buildMappedFundamentals } = require("../services/fundamentalsMapper.service");

const JOB_DELAY_MS = Number(process.env.FUNDAMENTALS_JOB_DELAY_MS || 5000);
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
  once: hasFlag("once", "single", "test"),
};

runtime.singleMode = Boolean(runtime.masterId || runtime.token || runtime.symbol || runtime.once);

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
  return updated;
};

const updateMasterFundamentalsStatus = async (masterId, payload = {}) => {
  if (!masterId) return null;
  const updatePayload = {
    screener_status: payload.screener_status || "PENDING",
  };
  try {
    return await stockMasterService.updateMasterStock(masterId, updatePayload);
  } catch (err) {
    return null;
  }
};

const fetchCandidatesFromDb = async (limit = CANDIDATE_LIMIT) => {
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
      security_code: m.security_code || null,
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
    if (String(master?.screener_status || "PENDING").toUpperCase() === "VALID") {
      throw new Error(`Screener status is ${String(master?.screener_status || "PENDING").toUpperCase()}`);
    }
    throw new Error("Stock is not eligible for screener fetch");
  }

  const active = await activeStockService.getActiveStockByMasterId(master.id);
  return [
    {
      master_id: String(master.id),
      active_stock_id: active?.id ? String(active.id) : null,
      name: master.name || null,
      symbol: master.symbol || null,
      screener_url: master.screener_url || null,
      security_code: master.security_code || null,
      force: true,
    },
  ];
};

const processCandidate = async (payload, position, total) => {
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
  const securityCode = master?.security_code || payload?.security_code || "";

  console.log(
    `[fundamentals] In progress ${position}/${total}: ${label} | run=${runtime.singleMode ? "single" : "batch"}`,
  );

  try {
    const result = await scrapeWithFallback(screenerUrl, { securityCode });
    await upsertFundamentals(payload, result.data);

    if (masterId && result.fallbackUsed && result.selectedUrl && result.selectedUrl !== screenerUrl) {
      await stockMasterService.updateMasterStock(masterId, {
        screener_url: result.selectedUrl,
      });
      console.log(
        `[fundamentals] Updated screener_url for ${label} => ${result.selectedUrl}`,
      );
    }

    await updateMasterFundamentalsStatus(masterId, {
      screener_status: "VALID",
    });

    return { ok: true, label, result };
  } catch (err) {
    const failedFields = Array.isArray(err?.failedFields) ? err.failedFields : null;
    await updateMasterFundamentalsStatus(masterId, {
      screener_status: "FAILED",
    });
    throw err;
  }
};

const runWorker = async () => {
  console.log("Fundamentals worker started (DB candidate mode)");
  let doneCount = 0;
  let failedCount = 0;
  let totalStarted = 0;

  while (true) {
    const candidates = runtime.singleMode
      ? await fetchSingleCandidate()
      : await fetchCandidatesFromDb();
    if (!candidates.length) {
      if (runtime.singleMode) {
        console.log("[fundamentals] No matching stock found for single-run mode");
        return;
      }
      await sleep(IDLE_DELAY_MS);
      continue;
    }

    const runTotal = candidates.length;
    console.log(`[fundamentals] DB candidates fetched: total=${runTotal}`);

    for (let i = 0; i < candidates.length; i += 1) {
      const payload = candidates[i];
      totalStarted += 1;

      try {
        const { label } = await processCandidate(payload, i + 1, runTotal);
        doneCount += 1;
        console.log(
          `[fundamentals] Done ${i + 1}/${runTotal}: ${label} | done=${doneCount}, failed=${failedCount}`,
        );
      } catch (err) {
        failedCount += 1;
        console.error(
          `[fundamentals] Failed ${i + 1}/${runTotal}: ${payload?.name || payload?.symbol || payload?.master_id || "unknown"} | done=${doneCount}, failed=${failedCount} | error=${
            err?.message || err
          }`,
        );
      }

      if (JOB_DELAY_MS > 0 && i < candidates.length - 1) {
        await sleep(JOB_DELAY_MS);
      }
    }

    if (runtime.singleMode) {
      return;
    }
  }
};

runWorker().catch((err) => {
  console.error("Fundamentals worker crashed", err);
  process.exit(1);
});
