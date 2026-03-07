const axios = require("axios");
require("dotenv").config();

const BASE_URL =
  process.env.TICKER_API_BASE_URL || "http://localhost:8000/api/v1/ticker";
const BATCH_SIZE = 50;
const WAIT_MS = 10_000;
const PAGE_LIMIT = 200;
const DEBUG = String(process.env.AUTO_ACTIVATE_DEBUG || "1") !== "0";

const client = axios.create({
  baseURL: BASE_URL,
  timeout: 60_000,
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const dbg = (...args) => {
  if (DEBUG) console.log("[debug]", ...args);
};

const normalizeToken = (value) => String(value ?? "").trim();

const isEqSymbol = (symbol) =>
  String(symbol ?? "")
    .toUpperCase()
    .endsWith("-EQ");

const parseTokenArgs = () => {
  const fromCli = process.argv
    .slice(2)
    .flatMap((arg) => {
      if (!arg) return [];
      if (arg.startsWith("--tokens=")) {
        return arg.slice("--tokens=".length).split(",");
      }
      return arg.split(",");
    })
    .map(normalizeToken)
    .filter(Boolean);

  const fromEnv = String(process.env.RAW_STOCK_TOKENS || "")
    .split(",")
    .map(normalizeToken)
    .filter(Boolean);

  return Array.from(new Set([...fromEnv, ...fromCli]));
};

async function fetchAllRawEqPending() {
  const out = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    const res = await client.get("/admin/raw-stocks", {
      params: {
        page,
        limit: PAGE_LIMIT,
        search: "-EQ",
      },
    });

    const rows = Array.isArray(res?.data?.data) ? res.data.data : [];
    const pagination = res?.data?.pagination || {};
    totalPages = Number(pagination.totalPages) || 1;

    const filtered = rows.filter(
      (r) =>
        isEqSymbol(r.symbol) &&
        String(r.status || "").toLowerCase() === "pending" &&
        normalizeToken(r.token),
    );

    out.push(...filtered);
    console.log(
      `Fetched raw page ${page}/${totalPages}: rows=${rows.length}, pendingEq=${filtered.length}`,
    );
    page += 1;
  }

  return out;
}

async function fetchRawStocksByTokens(tokens) {
  const tokenSet = new Set(tokens.map(normalizeToken));
  const out = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    const res = await client.get("/admin/raw-stocks", {
      params: {
        page,
        limit: PAGE_LIMIT,
        search: "",
      },
    });

    const rows = Array.isArray(res?.data?.data) ? res.data.data : [];
    const pagination = res?.data?.pagination || {};
    totalPages = Number(pagination.totalPages) || 1;

    for (const row of rows) {
      const token = normalizeToken(row.token);
      if (tokenSet.has(token)) out.push(row);
    }

    const foundTokens = new Set(out.map((r) => normalizeToken(r.token)));
    if (foundTokens.size === tokenSet.size) break;
    page += 1;
  }

  dbg(
    "Token mode rows:",
    out.map((r) => ({
      id: r.id,
      token: normalizeToken(r.token),
      symbol: r.symbol,
      exch_seg: r.exch_seg || r.exchange,
      status: r.status,
    })),
  );

  return out;
}

async function getPriceForBatch(batch) {
  const byExchange = new Map();
  for (const row of batch) {
    const exchange = String(row.exch_seg || row.exchange || "NSE");
    if (!byExchange.has(exchange)) byExchange.set(exchange, []);
    byExchange.get(exchange).push(row);
  }

  const fetchedByToken = new Map();
  const unfetchedTokenSet = new Set();

  for (const [exchange, rows] of byExchange.entries()) {
    const tokenIds = rows.map((r) => normalizeToken(r.token)).filter(Boolean);
    if (!tokenIds.length) continue;

    dbg(`Price request exchange=${exchange} tokenCount=${tokenIds.length}`, tokenIds);

    const res = await client.post("/admin/raw-stock-price", {
      mode: "OHLC",
      tokenIds,
      exchange,
    });

    const marketPayload = res?.data?.data?.data || {};
    dbg(
      `Price response exchange=${exchange} success=${res?.data?.success} fetchedCount=${
        marketPayload?.fetched?.length || 0
      } unfetchedCount=${marketPayload?.unfetched?.length || 0}`,
    );
    dbg(`Raw price response exchange=${exchange}`, JSON.stringify(res?.data, null, 2));

    const fetched = Array.isArray(marketPayload?.fetched) ? marketPayload.fetched : [];
    const rawUnfetched = Array.isArray(marketPayload?.unfetched) ? marketPayload.unfetched : [];
    const fetchedTokenSet = new Set();

    for (const item of fetched) {
      const token = normalizeToken(
        item?.symbolToken ?? item?.symboltoken ?? item?.token,
      );
      if (!token) continue;
      fetchedTokenSet.add(token);
      fetchedByToken.set(token, item);
    }

    dbg(
      `Fetched tokens exchange=${exchange}`,
      Array.from(fetchedTokenSet.values()),
    );
    dbg(`Unfetched payload exchange=${exchange}`, rawUnfetched);

    // Treat every requested-but-not-fetched token as unfetched/rejected.
    for (const token of tokenIds) {
      if (!fetchedTokenSet.has(token)) unfetchedTokenSet.add(token);
    }
  }

  return { fetchedByToken, unfetchedTokenSet };
}

async function updateRawViaMasterCreate(rawStockId, status) {
  try {
    dbg("Calling /master/create", { rawStockId, status });
    const res = await client.patch("/master/create", {
      rawStockId,
      status,
    });
    dbg("/master/create success", {
      rawStockId,
      status,
      httpStatus: res?.status,
      message: res?.data?.message,
    });

    return {
      ok: true,
      status: res?.status,
      message: res?.data?.message || "",
    };
  } catch (err) {
    const code = err?.response?.status;
    const message =
      err?.response?.data?.message ||
      err?.response?.data?.error?.message ||
      err?.message;

    // Existing stock / duplicate is acceptable for idempotent reruns.
    if (code === 409) {
      dbg("/master/create conflict treated ok", { rawStockId, status, message });
      return { ok: true, status: code, message: "Already exists" };
    }

    dbg("/master/create failed", { rawStockId, status, code, message });
    return { ok: false, status: code, message };
  }
}

async function run() {
  console.log(`Starting EQ auto-activation against ${BASE_URL}`);
  console.log(`Batch size=${BATCH_SIZE}, pause=${WAIT_MS / 1000}s`);

  const selectedTokens = parseTokenArgs();
  const runByTokens = selectedTokens.length > 0;
  const targetRows = runByTokens
    ? await fetchRawStocksByTokens(selectedTokens)
    : await fetchAllRawEqPending();

  if (!targetRows.length) {
    if (runByTokens) {
      console.log("No raw stocks found for provided tokens.");
    } else {
      console.log("No pending -EQ raw stocks found.");
    }
    return;
  }

  if (runByTokens) {
    const foundTokens = new Set(targetRows.map((r) => normalizeToken(r.token)));
    const missing = selectedTokens.filter((t) => !foundTokens.has(t));
    console.log(`Token mode: requested=${selectedTokens.length}, found=${targetRows.length}`);
    if (missing.length) {
      console.log(`Missing tokens: ${missing.join(", ")}`);
    }
  } else {
    console.log(`Total pending -EQ stocks: ${targetRows.length}`);
  }

  dbg(
    "Target rows summary:",
    targetRows.map((r) => ({
      id: r.id,
      token: normalizeToken(r.token),
      symbol: r.symbol,
      exch_seg: r.exch_seg || r.exchange,
      status: r.status,
    })),
  );

  let approved = 0;
  let rejected = 0;
  let failed = 0;
  let skippedInvalidPrice = 0;
  let batchNo = 0;

  for (let i = 0; i < targetRows.length; i += BATCH_SIZE) {
    batchNo += 1;
    const batch = targetRows.slice(i, i + BATCH_SIZE);
    console.log(
      `\nBatch ${batchNo}: processing ${batch.length} stocks (${i + 1}..${i + batch.length})`,
    );

    let priceResult;
    try {
      priceResult = await getPriceForBatch(batch);
    } catch (err) {
      failed += batch.length;
      console.error(
        `Batch ${batchNo} price fetch failed:`,
        err?.response?.data || err.message,
      );

      if (i + BATCH_SIZE < targetRows.length) {
        console.log(`Waiting ${WAIT_MS / 1000}s before next batch...`);
        await sleep(WAIT_MS);
      }
      continue;
    }

    for (const row of batch) {
      const token = normalizeToken(row.token);
      const fetched = priceResult.fetchedByToken.get(token);
      dbg("Evaluating row", {
        id: row.id,
        token,
        symbol: row.symbol,
        exch_seg: row.exch_seg || row.exchange,
        hasFetched: Boolean(fetched),
      });

      if (priceResult.unfetchedTokenSet.has(token)) {
        dbg("Decision=reject reason=token_unfetched", {
          id: row.id,
          token,
          symbol: row.symbol,
        });
        const res = await updateRawViaMasterCreate(row.id, "rejected");
        if (res.ok) rejected += 1;
        else {
          failed += 1;
          console.error(`Reject failed rawStockId=${row.id}: ${res.message}`);
        }
        continue;
      }

      const ltp = Number(
        fetched?.ltp ?? fetched?.close ?? fetched?.open ?? fetched?.high ?? 0,
      );
      dbg("Fetched price snapshot", {
        id: row.id,
        token,
        symbol: row.symbol,
        ltp: fetched?.ltp,
        close: fetched?.close,
        open: fetched?.open,
        high: fetched?.high,
        computedLtp: ltp,
      });
      if (!(ltp > 0)) {
        skippedInvalidPrice += 1;
        dbg("Decision=reject reason=invalid_price", {
          id: row.id,
          token,
          symbol: row.symbol,
          computedLtp: ltp,
        });
        const res = await updateRawViaMasterCreate(row.id, "rejected");
        if (res.ok) rejected += 1;
        else {
          failed += 1;
          console.error(`Reject failed rawStockId=${row.id}: ${res.message}`);
        }
        continue;
      }

      dbg("Decision=approve", {
        id: row.id,
        token,
        symbol: row.symbol,
        computedLtp: ltp,
      });
      const res = await updateRawViaMasterCreate(row.id, "approved");
      if (res.ok) approved += 1;
      else {
        failed += 1;
        console.error(`Approve failed rawStockId=${row.id}: ${res.message}`);
      }
    }

    console.log(
      `Batch ${batchNo} done. approved=${approved}, rejected=${rejected}, failed=${failed}`,
    );

    if (i + BATCH_SIZE < targetRows.length) {
      console.log(`Waiting ${WAIT_MS / 1000}s before next batch...`);
      await sleep(WAIT_MS);
    }
  }

  console.log("\nCompleted EQ auto-activation.");
  console.log({
    totalProcessed: targetRows.length,
    approved,
    rejected,
    failed,
    skippedInvalidPrice,
  });
}

run().catch((err) => {
  console.error("Fatal error:", err?.response?.data || err.message);
  process.exit(1);
});
