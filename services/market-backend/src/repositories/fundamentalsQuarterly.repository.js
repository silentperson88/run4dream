const { pool } = require("../config/db");
const { parseNumeric } = require("../services/fundamentalsMapper.service");
const { cleanLabel, normalizeLabel } = require("../config/fundamentalsRow.mapping");

const QUARTERLY_SOURCES = {
  sales: {
    column: "sales",
    children: {
      "YOY Sales Growth %": "sales_yoy_growth_percent",
    },
  },
  revenue: {
    column: "revenue",
    children: {
      "YOY Sales Growth %": "sales_yoy_growth_percent",
    },
  },
  financing_profit: {
    column: "financing_profit",
  },
  financing_margin_percent: {
    column: "financing_margin_percent",
  },
  raw_pdf: {
    column: "raw_pdf",
  },
  expenses: {
    column: "expenses",
    children: {
      "Material Cost %": "expenses_material_cost_percent",
      "Employee Cost %": "expenses_employee_cost_percent",
    },
  },
  operating_profit: {
    column: "operating_profit",
  },
  opm_percent: {
    column: "opm_percent",
  },
  other_income: {
    column: "other_income",
    children: {
      "Other income normal": "other_income_normal",
    },
  },
  interest: {
    column: "interest",
  },
  depreciation: {
    column: "depreciation",
  },
  profit_before_tax: {
    column: "profit_before_tax",
  },
  tax_percent: {
    column: "tax_percent",
  },
  net_profit: {
    column: "net_profit",
    children: {
      "Profit from Associates": "net_profit_profit_from_associates",
      "Minority share": "net_profit_minority_share",
      "Profit excl Excep": "net_profit_profit_excl_excep",
      "Profit for PE": "net_profit_profit_for_pe",
      "Profit for EPS": "net_profit_profit_for_eps",
      "Exceptional items": "net_profit_exceptional_items",
      "Exceptional items AT": "net_profit_exceptional_items_at",
      "YOY Profit Growth %": "net_profit_yoy_profit_growth_percent",
    },
  },
  eps: {
    column: "eps",
  },
  gross_npa_percent: {
    column: "gross_npa_percent",
  },
  net_npa_percent: {
    column: "net_npa_percent",
  },
};

const METRIC_KEYS = Object.keys(QUARTERLY_SOURCES);
const TEXT_COLUMNS = new Set(["raw_pdf"]);

const MONTH_MAP = {
  jan: "01",
  feb: "02",
  mar: "03",
  apr: "04",
  may: "05",
  jun: "06",
  jul: "07",
  aug: "08",
  sep: "09",
  oct: "10",
  nov: "11",
  dec: "12",
};

const normalizeForMatch = (label) => normalizeLabel(cleanLabel(label || ""));

const toTextOrNull = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    return text || null;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : null;
  }
  const text = String(value).trim();
  return text || null;
};

const normalizePeriodNumeric = (period) => {
  const label = cleanLabel(period || "");
  if (!label) return null;

  const monthYear = label.match(/^([A-Za-z]{3,9})\s+(\d{4})$/);
  if (monthYear) {
    const monthKey = monthYear[1].slice(0, 3).toLowerCase();
    const month = MONTH_MAP[monthKey];
    if (month) return `${month}-${monthYear[2]}`;
  }

  const numericLike = label.match(/^(\d{1,2})[-\/](\d{4})$/);
  if (numericLike) {
    return `${String(numericLike[1]).padStart(2, "0")}-${numericLike[2]}`;
  }

  return label.replace(/\s+/g, "-");
};

const ensureActiveStockId = async (master, activeStockService, snapshot = null) => {
  const direct = Number(snapshot?.active_stock_id || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;

  const existing = await activeStockService.getActiveStockByMasterId(master.id);
  if (existing?.id) return existing.id;

  const created = await activeStockService.addStock({
    master_id: master.id,
    token: master.token,
    symbol: master.symbol,
    name: master.name,
    exchange: master.exchange,
    security_code: master.security_code,
    instrumenttype: master.instrumenttype || "EQ",
  });

  return created?.id || null;
};

const resolveQuarterValue = (column, value) => {
  if (value === null || value === undefined) return null;
  if (TEXT_COLUMNS.has(column)) {
    return toTextOrNull(value);
  }
  return parseNumeric(value);
};

const getQuarterTable = (snapshot = {}) => {
  const candidates = [
    snapshot?.quarters_table,
    snapshot?.tables?.quarters,
    snapshot?.quarters,
    snapshot?.raw_payload?.quarters?.main_table,
  ];

  for (const candidate of candidates) {
    if (candidate && typeof candidate === "object") {
      return candidate;
    }
  }

  return {};
};

const getMetricRows = (quarterTable = {}) => {
  const rows = quarterTable?.rows;
  if (rows && typeof rows === "object" && !Array.isArray(rows)) {
    return rows;
  }

  if (!Array.isArray(rows)) {
    return {};
  }

  const out = {};
  rows.forEach((row) => {
    const key = row?.key || row?.metric_key || row?.row_key || row?.label_key || null;
    if (key) out[key] = row;
  });
  return out;
};

const extractQuarterRows = (snapshot, master, activeStockId = null) => {
  const quarterTable = getQuarterTable(snapshot);
  const headers = Array.isArray(quarterTable?.headers) ? quarterTable.headers : [];
  const periodHeaders = headers.slice();
  const rowsByKey = getMetricRows(quarterTable);
  const lastUpdatedAt = snapshot?.last_updated_at || snapshot?.updated_at || new Date();

  if (!periodHeaders.length) return [];

  const periods = periodHeaders.map((period, index) => {
    const periodLabel = cleanLabel(period) || null;
    const record = {
      master_id: Number(master.id),
      active_stock_id: Number(activeStockId || snapshot?.active_stock_id || 0) || null,
      snapshot_id: Number(snapshot?.id || 0) || null,
      period: periodLabel,
      period_numeric: normalizePeriodNumeric(period),
      period_label: periodLabel,
      period_end: null,
      period_index: index,
      title: quarterTable?.title || "Quarterly Performance",
      headers: JSON.stringify(headers),
      raw_row: JSON.stringify({
        period: periodLabel,
        period_numeric: normalizePeriodNumeric(period),
        metrics: {},
      }),
      row_label: periodLabel,
      last_updated_at: lastUpdatedAt,
      updated_at: new Date(),
    };

    METRIC_KEYS.forEach((metricKey) => {
      const source = QUARTERLY_SOURCES[metricKey];
      const row = rowsByKey?.[metricKey];
      if (!source || !row || !Array.isArray(row.values)) return;

      const parentValue = row.values[index];
      if (record[source.column] === undefined || record[source.column] === null) {
        const resolved = resolveQuarterValue(source.column, parentValue);
        if (resolved !== null) record[source.column] = resolved;
      }

      const childMap = source.children || {};
      const childRows = Array.isArray(row.children) ? row.children : [];
      childRows.forEach((child) => {
        const childLabel = normalizeForMatch(child?.title || child?.label || "");
        const targetColumn = Object.entries(childMap).find(
          ([label]) => normalizeForMatch(label) === childLabel,
        )?.[1];
        if (!targetColumn) return;

        const childValue = Array.isArray(child.values) ? child.values[index] : null;
        const resolvedChildValue = resolveQuarterValue(targetColumn, childValue);
        if (resolvedChildValue !== null && record[targetColumn] == null) {
          record[targetColumn] = resolvedChildValue;
        }
      });
    });

    Object.values(QUARTERLY_SOURCES).forEach((source) => {
      if (record[source.column] === undefined) record[source.column] = null;
      Object.values(source.children || {}).forEach((childColumn) => {
        if (record[childColumn] === undefined) record[childColumn] = null;
      });
    });

    return record;
  });

  return periods.filter((row) => row.period && row.period_numeric);
};

const QUARTERLY_COLUMNS = [
  "master_id",
  "active_stock_id",
  "snapshot_id",
  "period",
  "period_numeric",
  "period_label",
  "period_end",
  "period_index",
  "title",
  "headers",
  "raw_row",
  "row_label",
  "sales",
  "revenue",
  "financing_profit",
  "financing_margin_percent",
  "expenses",
  "interest",
  "net_profit",
  "opm_percent",
  "tax_percent",
  "depreciation",
  "other_income",
  "operating_profit",
  "profit_before_tax",
  "eps",
  "raw_pdf",
  "gross_npa_percent",
  "net_npa_percent",
  "sales_yoy_growth_percent",
  "expenses_material_cost_percent",
  "expenses_employee_cost_percent",
  "other_income_normal",
  "net_profit_profit_from_associates",
  "net_profit_minority_share",
  "net_profit_profit_excl_excep",
  "net_profit_profit_for_pe",
  "net_profit_profit_for_eps",
  "net_profit_exceptional_items",
  "net_profit_exceptional_items_at",
  "net_profit_yoy_profit_growth_percent",
  "last_updated_at",
  "updated_at",
];

const QUARTERLY_UPDATE_COLUMNS = QUARTERLY_COLUMNS.filter(
  (column) => !["master_id", "period_numeric", "created_at"].includes(column),
);

const upsertPeriods = async (master, snapshot, activeStockId, db = pool) => {
  const periods = extractQuarterRows(snapshot, master, activeStockId);
  if (!periods.length) {
    return { rows: [], count: 0 };
  }

  const values = [];
  const placeholders = periods.map((row, rowIndex) => {
    const offset = rowIndex * QUARTERLY_COLUMNS.length;
    const rowPlaceholders = QUARTERLY_COLUMNS.map((_, colIndex) => `$${offset + colIndex + 1}`);
    values.push(
      row.master_id,
      activeStockId,
      row.snapshot_id,
      row.period,
      row.period_numeric,
      row.period_label ?? row.period,
      row.period_end || null,
      row.period_index ?? null,
      row.title || null,
      row.headers || null,
      row.raw_row || null,
      row.row_label || null,
      row.sales ?? null,
      row.revenue ?? null,
      row.financing_profit ?? null,
      row.financing_margin_percent ?? null,
      row.expenses ?? null,
      row.interest ?? null,
      row.net_profit ?? null,
      row.opm_percent ?? null,
      row.tax_percent ?? null,
      row.depreciation ?? null,
      row.other_income ?? null,
      row.operating_profit ?? null,
      row.profit_before_tax ?? null,
      row.eps ?? null,
      row.raw_pdf ?? null,
      row.gross_npa_percent ?? null,
      row.net_npa_percent ?? null,
      row.sales_yoy_growth_percent ?? null,
      row.expenses_material_cost_percent ?? null,
      row.expenses_employee_cost_percent ?? null,
      row.other_income_normal ?? null,
      row.net_profit_profit_from_associates ?? null,
      row.net_profit_minority_share ?? null,
      row.net_profit_profit_excl_excep ?? null,
      row.net_profit_profit_for_pe ?? null,
      row.net_profit_profit_for_eps ?? null,
      row.net_profit_exceptional_items ?? null,
      row.net_profit_exceptional_items_at ?? null,
      row.net_profit_yoy_profit_growth_percent ?? null,
      row.last_updated_at || new Date(),
      row.updated_at || new Date(),
    );
    return `(${rowPlaceholders.join(", ")})`;
  });

  const updateSetClause = QUARTERLY_UPDATE_COLUMNS.map(
    (column) => `${column} = EXCLUDED.${column}`,
  ).join(", ");

  const { rows } = await db.query(
    `
      INSERT INTO stock_fundamental_quarterly_results (
        ${QUARTERLY_COLUMNS.join(", ")}
      )
      VALUES ${placeholders.join(", ")}
      ON CONFLICT (master_id, period_numeric)
      DO UPDATE SET
        ${updateSetClause}
      RETURNING id
    `,
    values,
  );

  return { rows, count: rows.length };
};

const getByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT *
      FROM stock_fundamental_quarterly_results
      WHERE master_id = $1
      ORDER BY period_numeric DESC, id DESC
    `,
    [Number(masterId)],
  );
  return rows;
};

const getBySymbol = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `
      SELECT q.*
      FROM stock_fundamental_quarterly_results q
      INNER JOIN stock_master sm ON sm.id = q.master_id
      WHERE sm.symbol = $1
      ORDER BY q.period_numeric DESC, q.id DESC
    `,
    [String(symbol || "").trim()],
  );
  return rows;
};

module.exports = {
  QUARTERLY_COLUMNS,
  QUARTERLY_SOURCES,
  extractQuarterRows,
  ensureActiveStockId,
  upsertPeriods,
  getByMasterId,
  getBySymbol,
};
