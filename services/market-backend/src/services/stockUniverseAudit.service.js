const fs = require("fs/promises");
const path = require("path");
const activeStockService = require("./activestock.service");
const { withTransaction } = require("../repositories/tx");
const { pool } = require("../config/db");

const candidateDirs = [
  path.resolve(process.cwd(), "BSE-NSE List"),
  path.resolve(process.cwd(), "../BSE-NSE List"),
  path.resolve(process.cwd(), "../../BSE-NSE List"),
  path.resolve(__dirname, "../../../../../BSE-NSE List"),
];

const normalize = (value) =>
  String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const normalizeSymbol = (value) => normalize(value);

const normalizeName = (value) =>
  normalize(String(value ?? "").replace(/\blimited\b/g, "ltd"));

const normalizeFileSymbol = (value) =>
  normalize(
    String(value ?? "")
      .trim()
      .replace(/[-_](eq|be|sm)$/i, ""),
  );

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

const buildLookup = (activeStocks = []) => {
  const bySymbol = new Map();
  const byName = new Map();

  for (const stock of activeStocks) {
    const symbolNormalized = normalizeFileSymbol(stock.symbol);
    const nameNormalized = normalizeName(stock.name);

    if (symbolNormalized) {
      bySymbol.set(symbolNormalized, stock);
    }
    if (nameNormalized) {
      byName.set(nameNormalized, stock);
    }
  }

  return { bySymbol, byName };
};

const matchActiveStock = (row, lookup) => {
  const symbol = row.symbol || row.SYMBOL || row["Security Id"] || row["Security Code"] || "";
  const name =
    row["NAME OF COMPANY"] || row["Security Name"] || row["Issuer Name"] || row.name || "";
  const symbolKey = normalizeFileSymbol(symbol);
  const nameKey = normalizeName(name);

  const checks = [
    ["symbol", lookup.bySymbol.get(symbolKey)],
    ["name", lookup.byName.get(nameKey)],
  ];

  for (const [matchedBy, stock] of checks) {
    if (stock) {
      return { stock, matchedBy };
    }
  }

  return { stock: null, matchedBy: null };
};

const buildUniverseAudit = async () => {
  const [activeStocks, files] = await Promise.all([
    activeStockService.getAllActiveStocks(),
    readAuditFiles(),
  ]);

  const lookup = buildLookup(activeStocks);

  const categories = {
    bseActiveMatched: [],
    nseActiveMatched: [],
    bseDelistedInActive: [],
    bseSuspendedInActive: [],
    bseMissingInActive: [],
    nseMissingInActive: [],
    activeStockNotInFiles: [],
    activeStockPriceSummary: [],
  };

  const seenActiveStockIds = new Set();

  const processRow = (row, exchange, source) => {
    const status = String(row.Status || row.status || "Active").trim();
    const symbol =
      row.SYMBOL || row["Security Id"] || row.symbol || row["Security Code"] || "";
    const name =
      row["NAME OF COMPANY"] || row["Security Name"] || row["Issuer Name"] || row.name || "";
    const match = matchActiveStock(row, lookup);
    const normalizedRow = {
      source,
      exchange,
      symbol: String(symbol).trim(),
      name: String(name).trim(),
      status,
      matched: Boolean(match.stock),
      matchedBy: match.matchedBy || "",
      activeStockId: match.stock?.id || null,
      activeStockSymbol: match.stock?.symbol || null,
      activeStockName: match.stock?.name || null,
    };

    if (match.stock?.id) {
      seenActiveStockIds.add(String(match.stock.id));
    }

    if (status.toLowerCase() === "delisted") {
      if (match.stock) {
        categories.bseDelistedInActive.push(normalizedRow);
      }
      return;
    }

    if (status.toLowerCase() === "suspended") {
      if (match.stock) {
        categories.bseSuspendedInActive.push(normalizedRow);
      }
      return;
    }

    if (match.stock) {
      if (exchange === "NSE") {
        categories.nseActiveMatched.push(normalizedRow);
      } else {
        categories.bseActiveMatched.push(normalizedRow);
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
        source: "Active Stock",
        exchange: String(stock.exchange || "").toUpperCase(),
        symbol: stock.symbol,
        name: stock.name,
        status: "active_stock_only",
        matched: false,
        matchedBy: "",
        activeStockId: stock.id,
        activeStockSymbol: stock.symbol,
        activeStockName: stock.name,
      });
    }

    categories.activeStockPriceSummary.push({
      source: "Active Stock",
      exchange: String(stock.exchange || "").toUpperCase(),
      symbol: stock.symbol,
      name: stock.name,
      status: stock.is_active ? "active" : "inactive",
      matched: true,
      matchedBy: "active_stock",
      activeStockId: stock.id,
      activeStockSymbol: stock.symbol,
      activeStockName: stock.name,
      ltp: Number(stock.ltp || 0),
      open: Number(stock.open || 0),
      high: Number(stock.high || 0),
      low: Number(stock.low || 0),
      close: Number(stock.close || 0),
    });
  }

  return {
    summary: {
      bseActiveMatched: categories.bseActiveMatched.length,
      nseActiveMatched: categories.nseActiveMatched.length,
      bseDelistedInActive: categories.bseDelistedInActive.length,
      bseSuspendedInActive: categories.bseSuspendedInActive.length,
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

  if (!conditions.length) {
    throw new Error("At least one filter value is required");
  }

  const sql = `
    SELECT id, master_id, token, symbol, name
    FROM active_stock
    WHERE ${conditions.join(" AND ")}
  `;

  const { rows } = await pool.query(sql, values);
  if (!rows.length) {
    return { updated: 0, matched: 0 };
  }

  const masterIds = Array.from(new Set(rows.map((row) => Number(row.master_id)).filter((n) => Number.isFinite(n) && n > 0)));
  const fundamentalsColumnCheck = await pool.query(
    `
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'stock_screener_fundamentals'
        AND column_name = 'is_active'
      LIMIT 1
    `,
  );
  const canMarkFundamentalsInactive = fundamentalsColumnCheck.rowCount > 0;

  await withTransaction(async (client) => {
    await client.query(
      `
        UPDATE stock_master
        SET is_active = FALSE, updated_at = NOW()
        WHERE id = ANY($1::bigint[])
      `,
      [masterIds],
    );

    await client.query(
      `
        UPDATE active_stock
        SET is_active = FALSE, last_update = NOW()
        WHERE master_id = ANY($1::bigint[])
      `,
      [masterIds],
    );

    if (canMarkFundamentalsInactive) {
      await client.query(
        `
          UPDATE stock_screener_fundamentals
          SET is_active = FALSE, updated_at = NOW()
          WHERE master_id = ANY($1::bigint[])
        `,
        [masterIds],
      );
    }
  });

  return {
    updated: masterIds.length,
    matched: rows.length,
    fundamentalsMarked: canMarkFundamentalsInactive,
  };
};

module.exports = {
  buildUniverseAudit,
  markFilteredStocksInactive,
};
