const fs = require("fs/promises");
const path = require("path");
const activeStockService = require("./activestock.service");
const rawstockService = require("./rawstock.service");
const stockMasterService = require("./stockMaster.service");
const stockFundamentalsService = require("./stockFundamental.service");
const rawstocksRepo = require("../repositories/rawstocks.repository");
const stockMasterRepo = require("../repositories/stockMaster.repository");
const activeStocksRepo = require("../repositories/activeStocks.repository");
const { withTransaction } = require("../repositories/tx");
const { pool } = require("../config/db");
const { normalizeSymbolForMatch } = require("../utils/stockSymbolMatch");

const candidateDirs = [
  path.resolve(process.cwd(), "BSE-NSE List"),
  path.resolve(process.cwd(), "../BSE-NSE List"),
  path.resolve(process.cwd(), "../../BSE-NSE List"),
  path.resolve(__dirname, "../../../../../BSE-NSE List"),
];

const splitCsvLine = (line) => {
  const out = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }

    current += ch;
  }

  out.push(current);
  return out.map((item) => item.trim());
};

const parseCsv = (text) => {
  const lines = String(text || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) return [];

  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = splitCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header.trim()] = values[index] ?? "";
    });
    return row;
  });
};

const readFirstExistingFile = async (fileNames) => {
  for (const dir of candidateDirs) {
    for (const fileName of fileNames) {
      const filePath = path.join(dir, fileName);
      try {
        const content = await fs.readFile(filePath, "utf8");
        return { filePath, content };
      } catch (_) {
        // try next
      }
    }
  }

  throw new Error(
    `Could not locate any of these files: ${fileNames.join(", ")} under ${candidateDirs.join(" | ")}`,
  );
};

const readAuditFiles = async () => {
  const [nseFile, bseFile] = await Promise.all([
    readFirstExistingFile(["EQUITY_L.csv", "equity_l.csv"]),
    readFirstExistingFile(["Equity.csv", "EQUITY.csv", "equity.csv"]),
  ]);

  return {
    nse: {
      filePath: nseFile.filePath,
      rows: parseCsv(nseFile.content),
    },
    bse: {
      filePath: bseFile.filePath,
      rows: parseCsv(bseFile.content),
    },
  };
};

const buildLookup = (stocks = []) => {
  const bySymbol = new Map();

  for (const stock of stocks) {
    const symbolNormalized = normalizeSymbolForMatch(stock.symbol);

    if (symbolNormalized) {
      bySymbol.set(symbolNormalized, stock);
    }
  }

  return { bySymbol };
};

const toPriceSnapshot = (stock = {}) => {
  const safeStock = stock || {};
  return {
    ltp: Number(safeStock.ltp || 0),
    open: Number(safeStock.open || 0),
    high: Number(safeStock.high || 0),
    low: Number(safeStock.low || 0),
    close: Number(safeStock.close || 0),
  };
};

const pickFirstNonEmpty = (...values) => {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
};

const getFileSymbolFromRow = (row = {}) =>
  pickFirstNonEmpty(
    row.SYMBOL,
    row.symbol,
    row["Security Id"],
    row["Security Code"],
    row["Security code"],
  );

const getFileNameFromRow = (row = {}) =>
  pickFirstNonEmpty(
    row["NAME OF COMPANY"],
    row["Security Name"],
    row["Issuer Name"],
    row.name,
  );

const getFileSecurityCodeFromRow = (row = {}) =>
  pickFirstNonEmpty(
    row["Security Code"],
    row["Security code"],
    row.securityCode,
    row.security_code,
    row["Security Id"],
    row["Security ID"],
    row["Security id"],
  );

const deriveSecurityCodeFromRow = (row = {}) => getFileSecurityCodeFromRow(row);

const matchesSymbolPattern = (symbol = "", pattern = "") => {
  const raw = String(symbol || "").trim().toUpperCase();
  const key = String(pattern || "").trim().toLowerCase();
  if (!key || key === "any") return true;

  const endsWith = (...suffixes) => suffixes.some((suffix) => raw.endsWith(suffix));

  switch (key) {
    case "suffix-be":
      return endsWith("-BE", " BE");
    case "suffix-bl":
      return endsWith("-BL", " BL");
    case "suffix-il":
      return endsWith("-IL", " IL");
    case "suffix-iq":
      return endsWith("-IQ", " IQ");
    case "suffix-iv":
      return endsWith("-IV", " IV");
    case "suffix-gr":
      return endsWith("GR", "-GR", " GR");
    case "prefix-sgb":
      return raw.startsWith("SGB");
    case "suffix-gb":
      return endsWith("GB", "-GB", " GB");
    case "suffix-n1n2":
      return endsWith("N1", "N2", "-N1", "-N2", " N1", " N2");
    case "suffix-pp":
      return endsWith("PP", "-PP", " PP");
    case "suffix-rs":
      return endsWith("RS", "-RS", " RS");
    case "suffix-sm":
      return endsWith("SM", "-SM", " SM");
    case "contains-niftysensex":
      return raw.includes("NIFTY") || raw.includes("SENSEX");
    default:
      return true;
  }
};

const matchActiveStock = (row, lookup) => {
  const symbol = getFileSymbolFromRow(row);
  const symbolKey = normalizeSymbolForMatch(symbol);
  const stock = symbolKey ? lookup.bySymbol.get(symbolKey) : null;

  if (stock) {
    return { stock, matchedBy: "symbol" };
  }

  return { stock: null, matchedBy: null };
};

const buildUniverseAudit = async () => {
  const [activeStocks, masterStocksRes, files] = await Promise.all([
    activeStockService.getAllActiveStocks(),
    stockMasterRepo.list({ page: 1, limit: 100000, is_active: undefined }),
    readAuditFiles(),
  ]);
  const masterStocks = Array.isArray(masterStocksRes?.data) ? masterStocksRes.data : [];

  const activeLookup = buildLookup(activeStocks);
  const masterLookup = buildLookup(masterStocks);

  const categories = {
    bseActiveMatched: [],
    nseActiveMatched: [],
    bseDelistedInActive: [],
    bseSuspendedInActive: [],
    bseMasterOnly: [],
    nseMasterOnly: [],
    bseMissingInActive: [],
    nseMissingInActive: [],
    activeStockNotInFiles: [],
    activeStockPriceSummary: [],
  };

  const seenActiveStockIds = new Set();

  const processRow = (row, exchange, source) => {
    const status = String(row.Status || row.status || "Active").trim();
    const fileSymbol = getFileSymbolFromRow(row);
    const fileName = getFileNameFromRow(row);
    const fileSecurityCode = getFileSecurityCodeFromRow(row);
    const masterMatch = matchActiveStock(row, masterLookup);
    const activeMatch = matchActiveStock(row, activeLookup);
    const normalizedRow = {
      source,
      exchange,
      symbol: fileSymbol,
      name: fileName,
      securityCode: fileSecurityCode || null,
      fileSymbol,
      fileName,
      fileSecurityCode: fileSecurityCode || null,
      status,
      matched: Boolean(masterMatch.stock),
      matchedBy: activeMatch.stock ? "active" : masterMatch.stock ? "master" : "",
      activeStockId: activeMatch.stock?.id || null,
      activeStockMasterId: activeMatch.stock?.master_id || masterMatch.stock?.id || null,
      activeStockSymbol: (activeMatch.stock || masterMatch.stock)?.symbol || null,
      activeStockName: (activeMatch.stock || masterMatch.stock)?.name || null,
      activeStockSecurityCode: (activeMatch.stock || masterMatch.stock)?.security_code || null,
      activeSymbol: (activeMatch.stock || masterMatch.stock)?.symbol || null,
      activeName: (activeMatch.stock || masterMatch.stock)?.name || null,
      activeSecurityCode: (activeMatch.stock || masterMatch.stock)?.security_code || null,
      ...toPriceSnapshot(activeMatch.stock || masterMatch.stock),
    };

    if (status.toLowerCase() === "delisted") {
      if (masterMatch.stock && activeMatch.stock) {
        categories.bseDelistedInActive.push(normalizedRow);
        seenActiveStockIds.add(String(activeMatch.stock.id));
      } else if (masterMatch.stock) {
        categories.bseMasterOnly.push(normalizedRow);
      }
      return;
    }

    if (status.toLowerCase() === "suspended") {
      if (masterMatch.stock && activeMatch.stock) {
        categories.bseSuspendedInActive.push(normalizedRow);
        seenActiveStockIds.add(String(activeMatch.stock.id));
      } else if (masterMatch.stock) {
        categories.bseMasterOnly.push(normalizedRow);
      }
      return;
    }

    if (masterMatch.stock && activeMatch.stock) {
      if (exchange === "NSE") {
        categories.nseActiveMatched.push(normalizedRow);
      } else {
        categories.bseActiveMatched.push(normalizedRow);
      }
      seenActiveStockIds.add(String(activeMatch.stock.id));
      return;
    }

    if (masterMatch.stock) {
      if (exchange === "NSE") {
        categories.nseMasterOnly.push(normalizedRow);
      } else {
        categories.bseMasterOnly.push(normalizedRow);
      }
      return;
    }

    if (exchange === "NSE") {
      categories.nseMissingInActive.push(normalizedRow);
    } else {
      categories.bseMissingInActive.push(normalizedRow);
    }
  };

  for (const row of files.nse.rows) {
    processRow(row, "NSE", "NSE Excel");
  }

  for (const row of files.bse.rows) {
    processRow(row, "BSE", "BSE Excel");
  }

  for (const stock of activeStocks) {
    if (!seenActiveStockIds.has(String(stock.id))) {
      categories.activeStockNotInFiles.push({
        source: "Stock Master",
        exchange: String(stock.exchange || "").toUpperCase(),
        symbol: stock.symbol || "",
        name: stock.name || "",
        securityCode: stock.security_code || null,
        fileSymbol: stock.symbol || "",
        fileName: stock.name || "",
        fileSecurityCode: stock.security_code || null,
        status: "active_stock_only",
        matched: false,
        matchedBy: "",
        activeStockId: stock.id,
        activeStockMasterId: stock.master_id || null,
        activeStockSymbol: stock.symbol,
        activeStockName: stock.name,
        activeStockSecurityCode: stock.security_code || null,
        activeSymbol: stock.symbol,
        activeName: stock.name,
        activeSecurityCode: stock.security_code || null,
        ...toPriceSnapshot(stock),
      });
    }

    const hasToken = String(stock.token || "").trim().length > 0;
    if (hasToken) {
      categories.activeStockPriceSummary.push({
        source: "Stock Master",
        exchange: String(stock.exchange || "").toUpperCase(),
        symbol: stock.symbol || "",
        name: stock.name || "",
        token: String(stock.token || "").trim() || null,
        securityCode: stock.security_code || null,
        fileSymbol: stock.symbol || "",
        fileName: stock.name || "",
        fileSecurityCode: stock.security_code || null,
        status: stock.master_is_active ? "active" : "inactive",
        matched: true,
        matchedBy: "active_stock",
        activeStockId: stock.id,
        activeStockMasterId: stock.master_id || null,
        activeStockSymbol: stock.symbol,
        activeStockName: stock.name,
        activeStockSecurityCode: stock.security_code || null,
        activeSymbol: stock.symbol,
        activeName: stock.name,
        activeSecurityCode: stock.security_code || null,
        ...toPriceSnapshot(stock),
      });
    }
  }

  return {
    summary: {
      bseActiveMatched: categories.bseActiveMatched.length,
      nseActiveMatched: categories.nseActiveMatched.length,
      bseDelistedInActive: categories.bseDelistedInActive.length,
      bseSuspendedInActive: categories.bseSuspendedInActive.length,
      bseMasterOnly: categories.bseMasterOnly.length,
      nseMasterOnly: categories.nseMasterOnly.length,
      bseMissingInActive: categories.bseMissingInActive.length,
      nseMissingInActive: categories.nseMissingInActive.length,
      activeStockNotInFiles: categories.activeStockNotInFiles.length,
      activeStockPriceSummary: categories.activeStockPriceSummary.length,
      totalActiveStocks: activeStocks.length,
      nseRows: files.nse.rows.length,
      bseRows: files.bse.rows.length,
      nseFile: files.nse.filePath,
      bseFile: files.bse.filePath,
    },
    categories,
  };
};

const markInactiveByMasterIds = async (masterIds = []) => {
  const ids = Array.from(
    new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)),
  );

  if (!ids.length) {
    return { updated: 0 };
  }

  await withTransaction(async (client) => {
    await client.query(
      `
        UPDATE stock_master
        SET is_active = FALSE, updated_at = NOW()
        WHERE id = ANY($1::bigint[])
      `,
      [ids],
    );
  });

  return {
    updated: ids.length,
  };
};

const markFilteredStocksInactive = async (filters = {}) => {
  const conditions = [];
  const values = [];
  const fields = ["ltp", "open", "high", "low", "close"];

  for (const field of fields) {
    const raw = filters[field];
    if (raw === undefined || raw === null || raw === "") continue;
    const num = Number(raw);
    if (!Number.isFinite(num)) {
      throw new Error(`Invalid filter value for ${field}`);
    }
    values.push(num);
    conditions.push(`COALESCE(${field}, 0) = $${values.length}`);
  }

  const exactMetric = String(filters.exactMetric || "").trim();
  const exactValue = filters.exactValue;
  if (exactMetric && fields.includes(exactMetric) && exactValue !== undefined && exactValue !== null && exactValue !== "") {
    const num = Number(exactValue);
    if (!Number.isFinite(num)) {
      throw new Error(`Invalid exact filter value for ${exactMetric}`);
    }
    values.push(num);
    conditions.push(`COALESCE(${exactMetric}, 0) = $${values.length}`);
  }

  if (String(filters.onlySamePrice || "").toLowerCase() === "true" || filters.onlySamePrice === true) {
    conditions.push(`
      COALESCE(ltp, 0) = COALESCE(open, 0)
      AND COALESCE(ltp, 0) = COALESCE(high, 0)
      AND COALESCE(ltp, 0) = COALESCE(low, 0)
      AND COALESCE(ltp, 0) = COALESCE(close, 0)
    `);
  }

  const ltpAbove = filters.ltpAbove;
  if (ltpAbove !== undefined && ltpAbove !== null && ltpAbove !== "") {
    const num = Number(ltpAbove);
    if (!Number.isFinite(num)) {
      throw new Error("Invalid LTP above filter value");
    }
    values.push(num);
    conditions.push(`COALESCE(ltp, 0) >= $${values.length}`);
  }

  if (!conditions.length) {
    throw new Error("At least one filter value is required");
  }

  const sql = `
    SELECT a.id, a.master_id, a.token, a.symbol, a.name
    FROM active_stock a
    INNER JOIN stock_master sm ON sm.id = a.master_id
    WHERE sm.is_active = TRUE
      AND ${conditions.join(" AND ")}
  `;

  const { rows } = await pool.query(sql, values);
  const symbolPattern = filters.symbolPattern;
  const symbolFilteredRows = rows.filter((row) => matchesSymbolPattern(row.symbol, symbolPattern));

  if (!symbolFilteredRows.length) {
    return { updated: 0, matched: 0 };
  }

  const masterIds = Array.from(
    new Set(symbolFilteredRows.map((row) => Number(row.master_id)).filter((n) => Number.isFinite(n) && n > 0)),
  );

  const inactiveResult = await markInactiveByMasterIds(masterIds);

  return {
    updated: inactiveResult.updated,
    matched: symbolFilteredRows.length,
  };
};

const markStocksInactiveByActiveStockIds = async (activeStockIds = []) => {
  const ids = Array.from(
    new Set(activeStockIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)),
  );

  if (!ids.length) {
    throw new Error("At least one active stock id is required");
  }

  const { rows } = await pool.query(
    `
      SELECT id, master_id
      FROM active_stock
      WHERE id = ANY($1::bigint[])
    `,
    [ids],
  );

  const masterIds = Array.from(
    new Set(rows.map((row) => Number(row.master_id)).filter((n) => Number.isFinite(n) && n > 0)),
  );

  const inactiveResult = await markInactiveByMasterIds(masterIds);

  return {
    matched: rows.length,
    ...inactiveResult,
  };
};

const deriveAuditStockPayload = (row = {}, exchangeHint = "") => {
  const exchange = String(row.exchange || exchangeHint || "").trim().toUpperCase();
  const symbol = String(row.symbol || row.SYMBOL || "").trim();
  const name = String(row.name || row["NAME OF COMPANY"] || row["Security Name"] || "").trim();
  const securityCode = deriveSecurityCodeFromRow(row);
  const token = String(row.token || "").trim() || null;

  if (!exchange || !symbol || !name) {
    throw new Error("exchange, symbol and name are required to add a stock");
  }

  return {
    token,
    symbol,
    name,
    exchange,
    security_code: securityCode || null,
    instrumenttype: String(row.instrumenttype || row.series || "EQ").trim().toUpperCase() || "EQ",
    lotsize: Number(row.lotsize || row.market_lot || 1) || 1,
    tick_size: row.tick_size ?? null,
  };
};

const addStockFromAuditRow = async (row = {}, exchangeHint = "") => {
  const payload = deriveAuditStockPayload(row, exchangeHint);

  const existingRawStock =
    (payload.token ? await rawstocksRepo.getByToken(payload.token) : null) ||
    (await rawstocksRepo.getByNormalizedSymbol(payload.symbol));

  const existingMaster =
    (payload.token ? await stockMasterRepo.getByToken(payload.token) : null) ||
    (await stockMasterRepo.getByNormalizedSymbol(payload.symbol));

  const existingMasterWasInactive = existingMaster ? existingMaster.is_active === false : false;

  const canonicalToken = existingMaster?.token || existingRawStock?.token || null;
  const existingActive =
    (existingMaster?.id && (await activeStocksRepo.getByMasterId(existingMaster.id))) ||
    null;

  if (existingActive && !existingMasterWasInactive) {
    return {
      status: "already_active",
      rawStockId: existingMaster?.raw_stock_id || existingRawStock?.id || null,
      masterId: existingMaster?.id || existingActive.master_id,
      activeStockId: existingActive.id,
    };
  }

  return withTransaction(async client => {
    const rawStock =
      existingRawStock ||
      (await rawstockService.createRawStock(
        {
          token: canonicalToken,
          symbol: payload.symbol,
          name: payload.name,
          exchange: payload.exchange,
          instrumenttype: payload.instrumenttype,
          lotsize: payload.lotsize,
          tick_size: payload.tick_size,
          security_code: payload.security_code,
          status: canonicalToken ? "approved" : "missing_token",
        },
        client,
      ));

    let masterStock =
      existingMaster ||
      (await stockMasterService.createMasterStock(
        {
          token: canonicalToken,
          symbol: payload.symbol,
          exchange: payload.exchange,
          name: payload.name,
          raw_stock_id: rawStock.id,
          screener_status: "PENDING",
          screener_url: "",
          security_code: payload.security_code,
        },
        client,
      ));

    if (existingMasterWasInactive && masterStock?.id) {
      masterStock = await stockMasterRepo.updateById(
        masterStock.id,
        { is_active: true },
        client,
      );
    }

    if (existingMaster && (existingMaster.raw_stock_id == null || existingMaster.security_code == null)) {
      await stockMasterRepo.updateById(
        existingMaster.id,
        {
          raw_stock_id: rawStock.id,
          security_code: existingMaster.security_code || payload.security_code,
        },
        client,
      );
    }

    const activeStock =
      existingActive ||
      (await activeStockService.addStock(
        {
          token: canonicalToken,
          symbol: payload.symbol,
          name: payload.name,
          exchange: payload.exchange,
          instrumenttype: payload.instrumenttype,
          master_id: masterStock.id,
          security_code: payload.security_code,
        },
        client,
      ));

    const existingFundamentals = await stockFundamentalsService.getFullStockFundamentals(masterStock.id);
    if (!existingFundamentals) {
      await stockFundamentalsService.createEntry(masterStock.id, activeStock.id, client);
    } else if (!existingFundamentals.active_stock_id || Number(existingFundamentals.active_stock_id) !== Number(activeStock.id)) {
      await stockFundamentalsService.linkActiveStockId(masterStock.id, activeStock.id, client);
    }

    return {
      status: existingMasterWasInactive ? "reactivated" : "created",
      rawStockId: rawStock.id,
      masterId: masterStock.id,
      activeStockId: activeStock.id,
      securityCode: payload.security_code,
    };
  });
};

const buildAuditRowKey = (row = {}) => {
  const symbol = normalizeSymbolForMatch(row.symbol || row.SYMBOL || "");
  return symbol ? `sym:${symbol}` : "";
};

const addStocksFromAuditRows = async (rows = [], exchangeHint = "") => {
  const seen = new Set();
  const results = [];
  let created = 0;
  let reactivated = 0;
  let alreadyActive = 0;
  let failed = 0;

  for (const row of rows) {
    const key = buildAuditRowKey(row);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);

    try {
      const result = await addStockFromAuditRow(row, exchangeHint || row.exchange || "");
      results.push({
        symbol: row.symbol || row.SYMBOL || "",
        name: row.name || row["NAME OF COMPANY"] || row["Security Name"] || "",
        status: result.status,
        masterId: result.masterId || null,
        activeStockId: result.activeStockId || null,
        rawStockId: result.rawStockId || null,
      });

      if (result.status === "created") {
        created += 1;
      } else if (result.status === "reactivated") {
        reactivated += 1;
      } else if (result.status === "already_active") {
        alreadyActive += 1;
      }
    } catch (err) {
      failed += 1;
      results.push({
        symbol: row.symbol || row.SYMBOL || "",
        name: row.name || row["NAME OF COMPANY"] || row["Security Name"] || "",
        status: "failed",
        error: err.message,
      });
    }
  }

  return {
    created,
    reactivated,
    alreadyActive,
    failed,
    total: results.length,
    results,
  };
};

module.exports = {
  buildUniverseAudit,
  markFilteredStocksInactive,
  markStocksInactiveByActiveStockIds,
  addStockFromAuditRow,
  addStocksFromAuditRows,
};
