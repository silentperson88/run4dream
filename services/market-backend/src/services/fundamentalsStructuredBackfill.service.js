const {
  cleanLabel,
  normalizeLabel,
} = require("../config/fundamentalsRow.mapping");
const { parseNumeric } = require("./fundamentalsMapper.service");

const MONTH_LOOKUP = {
  jan: 0,
  feb: 1,
  mar: 2,
  apr: 3,
  may: 4,
  jun: 5,
  jul: 6,
  aug: 7,
  sep: 8,
  oct: 9,
  nov: 10,
  dec: 11,
};

const normalizeKey = (value) => normalizeLabel(cleanLabel(value || ""));

const getByPath = (obj, path) => {
  if (!path) return null;
  return String(path)
    .split(".")
    .reduce((acc, segment) => {
      if (acc === null || acc === undefined) return null;
      if (Array.isArray(acc)) {
        const index = Number(segment);
        return Number.isInteger(index) ? acc[index] : null;
      }
      return acc?.[segment];
    }, obj);
};

const coerceText = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    return text && !["na", "n/a", "-", "--"].includes(text.toLowerCase())
      ? text
      : null;
  }
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    const text = value
      .map((item) => (item === null || item === undefined ? "" : String(item).trim()))
      .filter(Boolean)
      .join(" | ");
    return text || null;
  }
  if (typeof value === "object") {
    const text = JSON.stringify(value);
    return text === "{}" || text === "[]" ? null : text;
  }
  const text = String(value).trim();
  return text || null;
};

const coerceCellValue = (value) => {
  const numeric = parseNumeric(value);
  if (numeric !== null) return numeric;
  return coerceText(value);
};

const parsePeriodEnd = (label) => {
  if (!label) return null;
  const text = String(label).trim();
  const match = text.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})$/i);
  if (!match) return null;

  const month = MONTH_LOOKUP[match[1].toLowerCase()];
  const year = Number(match[2]);
  if (!Number.isInteger(month) || !Number.isFinite(year)) return null;

  const date = new Date(Date.UTC(year, month + 1, 0));
  return date.toISOString().slice(0, 10);
};

const buildSeriesIndex = (tableNode = {}) => {
  const index = new Map();
  const rows = tableNode?.rows || {};

  const addSeries = (label, values, meta = {}) => {
    const key = normalizeKey(label);
    if (!key || index.has(key)) return;
    index.set(key, {
      label: cleanLabel(label) || null,
      values: Array.isArray(values) ? values : [],
      ...meta,
    });
  };

  Object.entries(rows).forEach(([rowKey, row]) => {
    const parentLabel = row?.title || rowKey;
    addSeries(parentLabel, row?.values, { rowKey, kind: "row" });

    const children = Array.isArray(row?.children) ? row.children : [];
    children.forEach((child, childIndex) => {
      const childLabel = child?.title || child?.label || `${parentLabel}::child_${childIndex}`;
      addSeries(childLabel, child?.values, {
        rowKey,
        parentLabel,
        kind: "child",
      });
    });
  });

  return index;
};

const pickSeriesValue = (index, candidates = [], periodIndex) => {
  const candidateList = Array.isArray(candidates) ? candidates : [candidates];

  for (const candidate of candidateList) {
    const entry = index.get(normalizeKey(candidate));
    if (!entry) continue;
    const value = coerceCellValue(entry.values?.[periodIndex]);
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      return {
        value,
        source: entry.label,
      };
    }
  }

  return { value: null, source: null };
};

const pickPathValue = (snapshot, candidates = []) => {
  const candidateList = Array.isArray(candidates) ? candidates : [candidates];

  for (const candidate of candidateList) {
    const value = getByPath(snapshot, candidate);
    const resolved = coerceCellValue(value);
    if (resolved !== null && resolved !== undefined && String(resolved).trim() !== "") {
      return {
        value: resolved,
        source: candidate,
      };
    }
  }

  return { value: null, source: null };
};

const sectionColumnSources = {
  quarterly_results: [
    ["sales", ["Sales", "Revenue"]],
    ["revenue", ["Revenue", "Sales"]],
    ["financing_profit", ["Financing Profit"]],
    ["financing_margin_percent", ["Financing Margin %"]],
    ["raw_pdf", ["Raw PDF"]],
    ["expenses", ["Expenses"]],
    ["operating_profit", ["Operating Profit"]],
    ["opm_percent", ["OPM %"]],
    ["other_income", ["Other Income"]],
    ["other_income_normal", ["Other income normal"]],
    ["interest", ["Interest"]],
    ["depreciation", ["Depreciation"]],
    ["profit_before_tax", ["Profit before tax"]],
    ["tax_percent", ["Tax %"]],
    ["net_profit", ["Net Profit"]],
    ["eps", ["EPS in Rs"]],
    ["sales_growth_percent", ["YOY Sales Growth %", "Sales Growth %"]],
    ["yoy_sales_growth_percent", ["YOY Sales Growth %", "Sales Growth %"]],
    ["yoy_profit_growth_percent", ["YOY Profit Growth %", "Profit Growth %"]],
    ["profit_from_associates", ["Profit from Associates"]],
    ["minority_share", ["Minority share"]],
    ["exceptional_items", ["Exceptional items"]],
    ["exceptional_items_at", ["Exceptional items AT"]],
    ["profit_excl_excep", ["Profit excl Excep"]],
    ["profit_for_eps", ["Profit for EPS"]],
    ["profit_for_pe", ["Profit for PE"]],
    ["gross_npa_percent", ["Gross NPA %"]],
    ["net_npa_percent", ["Net NPA %"]],
  ],
  profit_loss: [
    ["sales", ["Sales", "Revenue"]],
    ["revenue", ["Revenue", "Sales"]],
    ["financing_profit", ["Financing Profit"]],
    ["financing_margin_percent", ["Financing Margin %"]],
    ["expenses", ["Expenses"]],
    ["operating_profit", ["Operating Profit"]],
    ["opm_percent", ["OPM %"]],
    ["other_income", ["Other Income"]],
    ["other_income_normal", ["Other income normal"]],
    ["interest", ["Interest"]],
    ["depreciation", ["Depreciation"]],
    ["profit_before_tax", ["Profit before tax"]],
    ["tax_percent", ["Tax %"]],
    ["net_profit", ["Net Profit"]],
    ["eps", ["EPS in Rs"]],
    ["dividend_payout_percent", ["Dividend Payout %"]],
    ["sales_growth_percent", ["Sales Growth %", "YOY Sales Growth %"]],
    ["yoy_sales_growth_percent", ["Sales Growth %", "YOY Sales Growth %"]],
    ["yoy_profit_growth_percent", ["Profit Growth %", "YOY Profit Growth %"]],
    ["profit_from_associates", ["Profit from Associates"]],
    ["minority_share", ["Minority share"]],
    ["exceptional_items", ["Exceptional items"]],
    ["exceptional_items_at", ["Exceptional items AT"]],
    ["profit_excl_excep", ["Profit excl Excep"]],
    ["profit_for_eps", ["Profit for EPS"]],
    ["profit_for_pe", ["Profit for PE"]],
    ["material_cost_percent", ["Material Cost %"]],
    ["employee_cost_percent", ["Employee Cost %"]],
    ["manufacturing_cost_percent", ["Manufacturing Cost %"]],
    ["other_cost_percent", ["Other Cost %"]],
  ],
  balance_sheet: [
    ["equity_capital", ["Equity Capital"]],
    ["reserves", ["Reserves"]],
    ["borrowing", ["Borrowing", "Borrowings"]],
    ["deposits", ["Deposits"]],
    ["borrowings", ["Borrowings", "Borrowing"]],
    ["long_term_borrowings", ["Long term Borrowings"]],
    ["short_term_borrowings", ["Short term Borrowings"]],
    ["other_borrowings", ["Other Borrowings"]],
    ["other_liabilities", ["Other Liabilities"]],
    ["advance_from_customers", ["Advance from Customers"]],
    ["lease_liabilities", ["Lease Liabilities"]],
    ["trade_payables", ["Trade Payables"]],
    ["other_liability_items", ["Other liability items"]],
    ["non_controlling_int", ["Non controlling int"]],
    ["total_liabilities", ["Total Liabilities"]],
    ["fixed_assets", ["Fixed Assets"]],
    ["gross_block", ["Gross Block"]],
    ["accumulated_depreciation", ["Accumulated Depreciation"]],
    ["building", ["Building"]],
    ["land", ["Land"]],
    ["plant_machinery", ["Plant Machinery"]],
    ["railway_sidings", ["Railway sidings"]],
    ["vehicles", ["Vehicles"]],
    ["computers", ["Computers"]],
    ["furniture_n_fittings", ["Furniture n fittings"]],
    ["equipments", ["Equipments"]],
    ["other_fixed_assets", ["Other fixed assets"]],
    ["intangible_assets", ["Intangible Assets"]],
    ["cwip", ["CWIP"]],
    ["investments", ["Investments"]],
    ["other_assets", ["Other Assets"]],
    ["inventories", ["Inventories"]],
    ["trade_receivables", ["Trade receivables"]],
    ["cash_equivalents", ["Cash Equivalents"]],
    ["loans_n_advances", ["Loans n Advances"]],
    ["other_asset_items", ["Other asset items"]],
    ["total_assets", ["Total Assets"]],
  ],
  cash_flow: [
    ["cash_from_operating_activity", ["Cash from Operating Activity"]],
    ["profit_from_operations", ["Profit from operations"]],
    ["working_capital_changes", ["Working capital changes"]],
    ["receivables", ["Receivables"]],
    ["inventory", ["Inventory"]],
    ["payables", ["Payables"]],
    ["other_wc_items", ["Other WC items"]],
    ["direct_taxes", ["Direct taxes"]],
    ["interest_received", ["Interest received"]],
    ["dividends_received", ["Dividends received"]],
    ["exceptional_cf_items", ["Exceptional CF items"]],
    ["operating_investments", ["Operating investments"]],
    ["operating_borrowings", ["Operating borrowings"]],
    ["operating_deposits", ["Operating Deposits"]],
    ["cash_from_investing_activity", ["Cash from Investing Activity"]],
    ["investments_purchased", ["Investments purchased"]],
    ["investments_sold", ["Investments sold"]],
    ["fixed_assets_purchased", ["Fixed assets purchased"]],
    ["fixed_assets_sold", ["Fixed assets sold"]],
    ["acquisition_of_companies", ["Acquisition of companies"]],
    ["invest_in_subsidiaries", ["Invest in subsidiaries"]],
    ["investment_in_group_cos", ["Investment in group cos"]],
    ["redemp_n_canc_of_shares", ["Redemp n Canc of Shares"]],
    ["loans_advances", ["Loans Advances"]],
    ["other_investing_items", ["Other investing items"]],
    ["cash_from_financing_activity", ["Cash from Financing Activity"]],
    ["proceeds_from_shares", ["Proceeds from shares"]],
    ["proceeds_from_borrowings", ["Proceeds from borrowings"]],
    ["repayment_of_borrowings", ["Repayment of borrowings"]],
    ["interest_paid_fin", ["Interest paid fin"]],
    ["dividends_paid", ["Dividends paid"]],
    ["financial_liabilities", ["Financial liabilities"]],
    ["share_application_money", ["Share application money"]],
    ["other_financing_items", ["Other financing items"]],
    ["net_cash_flow", ["Net Cash Flow"]],
  ],
  ratios: [
    ["debtor_days", ["Debtor Days"]],
    ["inventory_days", ["Inventory Days"]],
    ["days_payable", ["Days Payable"]],
    ["cash_conversion_cycle", ["Cash Conversion Cycle"]],
    ["working_capital_days", ["Working Capital Days"]],
    ["roce_percent", ["ROCE %"]],
    ["roe_percent", ["ROE %"]],
    ["gross_npa_percent", ["Gross NPA %"]],
    ["net_npa_percent", ["Net NPA %"]],
  ],
  shareholdings: [
    ["promoters", ["Promoters"]],
    ["fiis", ["FIIs"]],
    ["diis", ["DIIs"]],
    ["public", ["Public"]],
    ["others", ["Others"]],
    ["no_of_shareholders", ["No. of Shareholders"]],
  ],
};

const cagrFieldMap = [
  ["csg_10y", "compounded_sales_growth", "10 Years:"],
  ["csg_5y", "compounded_sales_growth", "5 Years:"],
  ["csg_3y", "compounded_sales_growth", "3 Years:"],
  ["csg_ttm", "compounded_sales_growth", "TTM:"],
  ["cpg_10y", "compounded_profit_growth", "10 Years:"],
  ["cpg_5y", "compounded_profit_growth", "5 Years:"],
  ["cpg_3y", "compounded_profit_growth", "3 Years:"],
  ["cpg_ttm", "compounded_profit_growth", "TTM:"],
  ["spc_10y", "stock_price_cagr", "10 Years:"],
  ["spc_5y", "stock_price_cagr", "5 Years:"],
  ["spc_3y", "stock_price_cagr", "3 Years:"],
  ["spc_1y", "stock_price_cagr", "1 Year:"],
  ["roe_10y", "return_on_equity", "10 Years:"],
  ["roe_5y", "return_on_equity", "5 Years:"],
  ["roe_3y", "return_on_equity", "3 Years:"],
  ["roe_last_year", "return_on_equity", "Last Year:"],
];

const sectionTableKeyMap = {
  quarterly_results: "quarters_table",
  profit_loss: "profit_loss_table",
  balance_sheet: "balance_sheet_table",
  cash_flow: "cash_flow_table",
  ratios: "ratios_table",
  shareholdings: "shareholdings_table",
};

const buildOverviewRow = (snapshot = {}) => {
  const companyInfo = snapshot?.company_info || {};
  const summary = snapshot?.summary || {};
  const otherDetails = snapshot?.other_details || {};
  const profitLossDetails = otherDetails?.profit_loss || {};

  const resolved = {};
  const sourcePayload = {
    company_info: companyInfo,
    summary,
    other_details: otherDetails,
  };

  [
    ["company_name", ["company_info.company_name", "company"]],
    ["about", ["company_info.about"]],
    ["key_points", ["company_info.key_points"]],
    ["market_cap", ["summary.market_snapshot.market_cap"]],
    ["current_price", ["summary.market_snapshot.current_price"]],
    ["high_low", ["summary.market_snapshot.high_low"]],
    ["stock_pe", ["summary.market_snapshot.stock_pe", "summary.market_snapshot.pe_ratio"]],
    ["book_value", ["summary.market_snapshot.book_value"]],
    ["dividend_yield", ["summary.market_snapshot.dividend_yield"]],
    ["roce", ["summary.market_snapshot.roce"]],
    ["roe", ["summary.market_snapshot.roe"]],
    ["face_value", ["summary.market_snapshot.face_value"]],
  ].forEach(([key, paths]) => {
    const { value, source } = pickPathValue(snapshot, paths);
    resolved[key] = value;
    if (source) sourcePayload[key] = source;
  });

  const directEntryByKey = Object.fromEntries(
    Object.entries(profitLossDetails).flatMap(([sectionKey, sectionValue]) => {
      const entries = Array.isArray(sectionValue?.entries) ? sectionValue.entries : [];
      return entries.map((entry) => [entry?.key, entry?.value ?? null]);
    }),
  );
  cagrFieldMap.forEach(([columnKey]) => {
    resolved[columnKey] = coerceText(directEntryByKey[columnKey]);
  });

  return {
    company_name: resolved.company_name,
    about: coerceText(resolved.about),
    key_points: coerceText(resolved.key_points),
    market_cap: resolved.market_cap,
    current_price: resolved.current_price,
    high_low: coerceText(resolved.high_low),
    stock_pe: resolved.stock_pe,
    book_value: resolved.book_value,
    dividend_yield: resolved.dividend_yield,
    roce: resolved.roce,
    roe: resolved.roe,
    face_value: resolved.face_value,
    csg_10y: coerceText(directEntryByKey.csg_10y),
    csg_5y: coerceText(directEntryByKey.csg_5y),
    csg_3y: coerceText(directEntryByKey.csg_3y),
    csg_ttm: coerceText(directEntryByKey.csg_ttm),
    cpg_10y: coerceText(directEntryByKey.cpg_10y),
    cpg_5y: coerceText(directEntryByKey.cpg_5y),
    cpg_3y: coerceText(directEntryByKey.cpg_3y),
    cpg_ttm: coerceText(directEntryByKey.cpg_ttm),
    spc_10y: coerceText(directEntryByKey.spc_10y),
    spc_5y: coerceText(directEntryByKey.spc_5y),
    spc_3y: coerceText(directEntryByKey.spc_3y),
    spc_1y: coerceText(directEntryByKey.spc_1y),
    roe_10y: coerceText(directEntryByKey.roe_10y),
    roe_5y: coerceText(directEntryByKey.roe_5y),
    roe_3y: coerceText(directEntryByKey.roe_3y),
    roe_last_year: coerceText(directEntryByKey.roe_last_year),
    pros: Array.isArray(summary?.pros) ? summary.pros : [],
    cons: Array.isArray(summary?.cons) ? summary.cons : [],
    links: Array.isArray(companyInfo?.links) ? companyInfo.links : [],
    source_payload: sourcePayload,
  };
};

const buildPeersRow = (snapshot = {}) => {
  const peers = snapshot?.peers || {};
  const table = peers?.main_table || {};
  return {
    title: coerceText(table?.title) || "Peers",
    headers: Array.isArray(table?.headers) ? table.headers : [],
    rows: Array.isArray(table?.rows) ? table.rows : [],
    table_class: coerceText(table?.table_class),
    source_payload: { peers },
  };
};

const buildSectionRows = (snapshot = {}, sectionKey) => {
  const sectionTable = snapshot?.[sectionTableKeyMap[sectionKey] || `${sectionKey}_table`] || {};
  const headers = Array.isArray(sectionTable?.headers) ? sectionTable.headers : [];
  const index = buildSeriesIndex(sectionTable);
  const columnDefs = sectionColumnSources[sectionKey] || [];
  const cagrSourcePayload = snapshot?.other_details?.profit_loss || {};
  const rows = [];

  headers.forEach((periodLabel, periodIndex) => {
    const resolved = {};
    const matchedSources = {};

    columnDefs.forEach(([columnKey, candidates]) => {
      const { value, source } = pickSeriesValue(index, candidates, periodIndex);
      resolved[columnKey] = value;
      matchedSources[columnKey] = source;
    });

    // Reuse the same stock-wide CAGR details on every profit/loss row so the
    // structured table remains self-contained for now.
    if (sectionKey === "profit_loss") {
      const directEntryByKey = Object.fromEntries(
        Object.entries(cagrSourcePayload).flatMap(([sectionName, sectionValue]) => {
          const entries = Array.isArray(sectionValue?.entries) ? sectionValue.entries : [];
          return entries.map((entry) => [entry?.key, entry?.value ?? null]);
        }),
      );
      cagrFieldMap.forEach(([columnKey]) => {
        if (resolved[columnKey] !== null && resolved[columnKey] !== undefined) return;
        resolved[columnKey] = coerceText(directEntryByKey[columnKey]);
      });
    }

    const rawRow = {
      period_label: periodLabel,
      period_index: periodIndex,
      headers,
      values: resolved,
      matched_sources: matchedSources,
    };

    const baseRow = {
      period_label: coerceText(periodLabel) || `period_${periodIndex + 1}`,
      period_end: parsePeriodEnd(periodLabel),
      period_index: periodIndex,
      title: coerceText(sectionTable?.title) || null,
      headers,
      raw_row: rawRow,
      row_label: coerceText(periodLabel) || null,
      last_updated_at: snapshot?.last_updated_at || snapshot?.updated_at || new Date(),
    };

    if (sectionKey === "shareholdings") {
      const children = [];
      Array.from(index.values())
        .filter((entry) => entry?.kind === "child")
        .forEach((entry) => {
          children.push({
            title: entry.label,
            parent_label: entry.parentLabel,
            values: Array.isArray(entry.values) ? entry.values : [],
          });
        });
      baseRow.children = children;
    }

    rows.push({
      ...baseRow,
      ...resolved,
    });
  });

  return rows;
};

const buildStructuredPayload = (snapshot = {}) => {
  const overview = buildOverviewRow(snapshot);
  const peers = buildPeersRow(snapshot);
  const quarterlyResults = buildSectionRows(snapshot, "quarterly_results");
  const profitLoss = buildSectionRows(snapshot, "profit_loss");
  const balanceSheet = buildSectionRows(snapshot, "balance_sheet");
  const cashFlow = buildSectionRows(snapshot, "cash_flow");
  const ratios = buildSectionRows(snapshot, "ratios");
  const shareholdings = buildSectionRows(snapshot, "shareholdings");

  return {
    overview,
    peers,
    quarterly_results: quarterlyResults,
    profit_loss: profitLoss,
    balance_sheet: balanceSheet,
    cash_flow: cashFlow,
    ratios,
    shareholdings,
  };
};

const serializeDbValue = (value) => {
  if (value === undefined) return null;
  if (value === null) return null;
  if (value instanceof Date) return value;
  if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
    return JSON.stringify(value);
  }
  return value;
};

const quoteIdent = (ident) => `"${String(ident).replace(/"/g, '""')}"`;

const upsertStructuredRow = async (db, tableName, data, conflictColumns) => {
  const entries = Object.entries(data).filter(([, value]) => value !== undefined);
  const columns = entries.map(([key]) => quoteIdent(key));
  const values = entries.map(([, value]) => serializeDbValue(value));
  const updateColumns = entries
    .map(([key]) => key)
    .filter((key) => !conflictColumns.includes(key) && key !== "created_at");

  const sql = `
    INSERT INTO ${quoteIdent(tableName)} (${columns.join(", ")})
    VALUES (${entries.map((_, idx) => `$${idx + 1}`).join(", ")})
    ON CONFLICT (${conflictColumns.map(quoteIdent).join(", ")})
    DO UPDATE SET ${updateColumns
      .map((key) => `${quoteIdent(key)} = EXCLUDED.${quoteIdent(key)}`)
      .join(", ")}
    RETURNING id
  `;

  const { rows } = await db.query(sql, values);
  return rows[0] || null;
};

module.exports = {
  buildStructuredPayload,
  upsertStructuredRow,
  buildSeriesIndex,
  pickSeriesValue,
  pickPathValue,
  parsePeriodEnd,
  coerceCellValue,
  coerceText,
};
