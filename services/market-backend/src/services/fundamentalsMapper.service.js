const { fundamentalsMapping, cleanLabel, normalizeLabel } = require("../config/fundamentalsRow.mapping");

const normalizeForMatch = (label) => normalizeLabel(cleanLabel(label || ""));

const parseNumeric = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const text = String(value).trim();
  if (!text) return null;
  if (["na", "n/a", "-", "--"].includes(text.toLowerCase())) return null;

  const cleaned = text
    .replace(/,/g, "")
    .replace(/₹/g, "")
    .replace(/Rs\.?/gi, "")
    .replace(/Cr\.?/gi, "")
    .replace(/%/g, "")
    .trim();

  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
};

const parseCellValue = (value) => {
  const num = parseNumeric(value);
  if (num !== null) return num;
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text || ["na", "n/a", "-", "--"].includes(text.toLowerCase())) return null;
  return text;
};

const initMetric = (label, headers) => ({
  title: label || null,
  values: headers.map(() => null),
  children: [],
});

const setSeriesValues = (targetValues, sourceRow, headers) => {
  headers.forEach((header, index) => {
    targetValues[index] = parseCellValue(sourceRow?.[header]);
  });
};

const buildLabelIndex = (rowsConfig = []) => {
  const index = new Map();
  rowsConfig.forEach((rowConfig) => {
    if (!rowConfig?.key || !rowConfig?.label) return;
    index.set(normalizeForMatch(rowConfig.label), rowConfig.key);
    (rowConfig.aliases || []).forEach((alias) => {
      index.set(normalizeForMatch(alias), rowConfig.key);
    });
  });
  return index;
};

const mapTable = (tableNode, rowsConfig = [], fallbackTitle = null) => {
  const table = tableNode?.main_table || {};
  const allHeaders = Array.isArray(table.headers) ? table.headers : [];
  const headers = allHeaders.slice(1);
  const sourceRows = Array.isArray(table.rows) ? table.rows : [];

  const rows = {};

  const labelIndex = buildLabelIndex(rowsConfig);
  const configByKey = new Map(
    rowsConfig
      .filter((cfg) => cfg?.key)
      .map((cfg) => [cfg.key, cfg]),
  );
  const unmatched = [];

  sourceRows.forEach((sourceRow) => {
    const rowLabel = sourceRow?.label || sourceRow?.[allHeaders[0]] || "";
    const rowKey = labelIndex.get(normalizeForMatch(rowLabel));
    if (!rowKey) {
      unmatched.push(sourceRow);
      return;
    }

    if (!rows[rowKey]) {
      const cfg = configByKey.get(rowKey);
      rows[rowKey] = initMetric(cfg?.label || cleanLabel(rowLabel), headers);
    }

    setSeriesValues(rows[rowKey].values, sourceRow, headers);
    const childRows = Array.isArray(sourceRow?.children) ? sourceRow.children : [];

    childRows.forEach((child) => {
      const childLabel = child?.label || child?.[allHeaders[0]] || "";
      const dynamicChild = {
        title: cleanLabel(childLabel) || null,
        values: headers.map(() => null),
      };
      setSeriesValues(dynamicChild.values, child, headers);

      const hasAnyValue = dynamicChild.values.some((v) => v !== null);
      const hasValidTitle = dynamicChild.title && dynamicChild.title !== "<dynamic_holder_rows>";
      if (hasAnyValue && hasValidTitle) {
        rows[rowKey].children.push(dynamicChild);
      }
    });
  });

  return {
    title: table?.title || fallbackTitle,
    headers,
    rows,
    unmatched_rows: unmatched,
  };
};

const mapProfitLossOtherDetails = (otherDetails = []) => {
  const details = Array.isArray(otherDetails) ? otherDetails : [];
  const sectionMap = fundamentalsMapping?.profit_loss?.other_details || {};

  const result = Object.fromEntries(
    Object.entries(sectionMap).map(([sectionKey, cfg]) => [
      sectionKey,
      { title: cfg?.label || sectionKey, entries: [] },
    ]),
  );

  details.forEach((table) => {
    const headers = Array.isArray(table?.headers) ? table.headers : [];
    const tableTitle = table?.title || null;
    const singleHeaderTitle = headers.length === 1 ? headers[0] : null;
    const matcherTitle = tableTitle || singleHeaderTitle;

    const sectionEntry = Object.entries(sectionMap).find(
      ([, cfg]) => normalizeForMatch(cfg?.label) === normalizeForMatch(matcherTitle),
    );
    if (!sectionEntry) return;

    const [sectionKey, cfg] = sectionEntry;
    const rows = Array.isArray(table?.rows) ? table.rows : [];
    rows.forEach((row) => {
      const periodLabel = row?.label || row?.[headers[0]];
      const mappedKey = cfg?.periods?.[periodLabel];
      if (!mappedKey) return;

      // Works for:
      // 1) { label: "10 Years:", value: "21%" }
      // 2) { "10 Years:": "21%" } with headers ["label","value"] variants
      // 3) fallback single-value column tables
      const rawValue =
        row?.value ??
        (headers[1] ? row?.[headers[1]] : undefined) ??
        null;
      const rawText =
        rawValue === null || rawValue === undefined ? null : String(rawValue).trim();

      result[sectionKey].entries.push({
        title: periodLabel,
        key: mappedKey,
        value: rawText,
      });
    });
  });

  return result;
};

const mapPeersTable = (peersNode = {}) => {
  const table = peersNode?.main_table || {};
  const headers = Array.isArray(table.headers) ? table.headers : [];
  const rows = Array.isArray(table.rows) ? table.rows : [];
  const firstHeader = headers[0];

  return {
    title: "Peers",
    headers,
    rows: rows.map((row) => headers.map((h, idx) => {
      if (idx === 0 && row?.label !== undefined) return row.label;
      if (row?.[h] !== undefined) return row[h];
      if (idx === 0 && firstHeader && row?.[firstHeader] !== undefined) return row[firstHeader];
      return null;
    })),
    table_class: table?.table_class || null,
  };
};

const mapMarketSnapshot = (companyInfo = {}) => {
  const ratios = Array.isArray(companyInfo?.top_ratios) ? companyInfo.top_ratios : [];
  const ratioValueByKey = {};
  const topRatioMap = fundamentalsMapping?.company_info?.top_ratios || [];

  ratios.forEach((item) => {
    const key = topRatioMap.find((ratioCfg) => {
      if (normalizeForMatch(ratioCfg.label) === normalizeForMatch(item?.name)) return true;
      return (ratioCfg.aliases || []).some(
        (alias) => normalizeForMatch(alias) === normalizeForMatch(item?.name),
      );
    })?.key;
    if (key) ratioValueByKey[key] = item?.value ?? null;
  });

  return {
    market_cap: parseNumeric(ratioValueByKey.market_cap),
    current_price: parseNumeric(ratioValueByKey.current_price),
    high_low: ratioValueByKey.high_low || null,
    stock_pe: parseNumeric(ratioValueByKey.stock_pe),
    pe_ratio: parseNumeric(ratioValueByKey.stock_pe),
    book_value: parseNumeric(ratioValueByKey.book_value),
    dividend_yield: parseNumeric(ratioValueByKey.dividend_yield),
    roce: parseNumeric(ratioValueByKey.roce),
    roe: parseNumeric(ratioValueByKey.roe),
    face_value: parseNumeric(ratioValueByKey.face_value),
    debt_free: null,
  };
};

const buildMappedFundamentals = (raw = {}) => ({
  company: raw?.company_info?.company_name || null,
  company_info: {
    about: raw?.company_info?.about || null,
    key_points: raw?.company_info?.key_points || null,
    links: Array.isArray(raw?.company_info?.links) ? raw.company_info.links : [],
  },
  summary: {
    market_snapshot: mapMarketSnapshot(raw?.company_info || {}),
    pros: Array.isArray(raw?.analysis?.pros_cons?.pros) ? raw.analysis.pros_cons.pros : [],
    cons: Array.isArray(raw?.analysis?.pros_cons?.cons) ? raw.analysis.pros_cons.cons : [],
  },
  peers: {
    main_table: mapPeersTable(raw?.peers || {}),
  },
  tables: {
    quarters: mapTable(raw?.quarters, fundamentalsMapping?.quarters?.rows || [], "Quarterly Results"),
    profit_loss: mapTable(raw?.profit_loss, fundamentalsMapping?.profit_loss?.rows || [], "Profit & Loss"),
    balance_sheet: mapTable(raw?.balance_sheet, fundamentalsMapping?.balance_sheet?.rows || [], "Balance Sheet"),
    cash_flow: mapTable(raw?.cash_flow, fundamentalsMapping?.cash_flow?.rows || [], "Cash Flow"),
    ratios: mapTable(raw?.ratios, fundamentalsMapping?.ratios?.rows || [], "Ratios"),
    shareholdings: mapTable(raw?.shareholdings, fundamentalsMapping?.shareholdings?.rows || [], "Shareholding Pattern"),
  },
  other_details: {
    profit_loss: mapProfitLossOtherDetails(raw?.profit_loss?.other_details || []),
  },
  documents: raw?.documents || {},
  raw_payload: raw,
});

module.exports = {
  parseNumeric,
  buildMappedFundamentals,
};
