const { pool } = require("../config/db");
const { parseNumeric } = require("../services/fundamentalsMapper.service");
const { cleanLabel, normalizeLabel } = require("../config/fundamentalsRow.mapping");
const { SPLIT_TABLES } = require("../config/fundamentalsSplitTables.config");

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

const normalizeForMatch = (label) => normalizeLabel(cleanLabel(label || ""));

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

const getTable = (snapshot, tableDef) => {
  const candidates = [
    snapshot?.[tableDef.rawKey],
    snapshot?.tables?.[tableDef.rawKey?.replace(/_table$/, "")],
    snapshot?.raw_payload?.[tableDef.rawKey?.replace(/_table$/, "")]?.main_table,
  ];

  for (const candidate of candidates) {
    if (candidate && typeof candidate === "object") return candidate;
  }

  return {};
};

const getRowsByKey = (table = {}) => {
  const rows = table?.rows;
  if (rows && typeof rows === "object" && !Array.isArray(rows)) return rows;
  if (!Array.isArray(rows)) return {};

  const out = {};
  rows.forEach((row) => {
    const key = row?.key || row?.metric_key || row?.row_key || row?.label_key || null;
    if (key) out[key] = row;
  });
  return out;
};

const resolveValue = (column, value) => {
  if (value === null || value === undefined) return null;
  if (column === "raw_pdf" || column === "period" || column === "period_numeric") {
    return toTextOrNull(value);
  }
  return parseNumeric(value);
};

const buildSectionColumns = (sectionDef) => {
  const columns = [
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
  ];

  sectionDef.rows.forEach((row) => {
    if (!columns.includes(row.key)) columns.push(row.key);
    (row.children || []).forEach((child) => {
      if (!columns.includes(child.column)) columns.push(child.column);
    });
  });

  columns.push("last_updated_at", "updated_at");
  return columns;
};

const buildPeriodRows = (sectionKey, snapshot, master, activeStockId = null) => {
  const sectionDef = SPLIT_TABLES[sectionKey];
  if (!sectionDef) return [];

  const table = getTable(snapshot, sectionDef);
  const headers = Array.isArray(table?.headers) ? table.headers : [];
  const rowsByKey = getRowsByKey(table);
  if (!headers.length || !Object.keys(rowsByKey).length) return [];

  const serializedHeaders = JSON.stringify(headers);

  return headers.map((period, index) => {
    const periodLabel = cleanLabel(period) || null;
    const periodSource = {};
    sectionDef.rows.forEach((rowDef) => {
      const source = rowsByKey?.[rowDef.key];
      if (!source || !Array.isArray(source.values)) {
        periodSource[rowDef.key] = null;
        return;
      }

      periodSource[rowDef.key] = source.values[index] ?? null;
      (rowDef.children || []).forEach((child) => {
        const childRows = Array.isArray(source.children) ? source.children : [];
        const childMatch = childRows.find(
          (candidate) => normalizeForMatch(candidate?.title || candidate?.label || "") === normalizeForMatch(child.label),
        );
        if (!childMatch) {
          periodSource[child.column] = null;
          return;
        }
        periodSource[child.column] = Array.isArray(childMatch.values) ? childMatch.values[index] ?? null : null;
      });
    });

    const record = {
      master_id: Number(master.id),
      active_stock_id: Number(activeStockId || snapshot?.active_stock_id || 0) || null,
      snapshot_id: Number(snapshot?.id || 0) || null,
      period: periodLabel,
      period_numeric: normalizePeriodNumeric(period),
      period_label: periodLabel,
      period_end: null,
      period_index: index,
      title: sectionDef.label || null,
      headers: serializedHeaders,
      raw_row: JSON.stringify({
        period: periodLabel,
        period_numeric: normalizePeriodNumeric(period),
        values: periodSource,
      }),
      row_label: periodLabel,
      last_updated_at: snapshot?.last_updated_at || snapshot?.updated_at || new Date(),
      updated_at: new Date(),
    };

    sectionDef.rows.forEach((rowDef) => {
      const source = rowsByKey?.[rowDef.key];
      if (!source || !Array.isArray(source.values)) {
        if (record[rowDef.key] === undefined) record[rowDef.key] = null;
        (rowDef.children || []).forEach((child) => {
          if (record[child.column] === undefined) record[child.column] = null;
        });
        return;
      }

      const parentValue = source.values[index];
      if (record[rowDef.key] === undefined || record[rowDef.key] === null) {
        const resolved = resolveValue(rowDef.key, parentValue);
        if (resolved !== null) record[rowDef.key] = resolved;
      }

      const childRows = Array.isArray(source.children) ? source.children : [];
      childRows.forEach((child) => {
        const childLabel = normalizeForMatch(child?.title || child?.label || "");
        const childDef = (rowDef.children || []).find(
          (candidate) => normalizeForMatch(candidate.label) === childLabel,
        );
        if (!childDef) return;
        const childValue = Array.isArray(child.values) ? child.values[index] : null;
        const resolvedChildValue = resolveValue(childDef.column, childValue);
        if (resolvedChildValue !== null && record[childDef.column] == null) {
          record[childDef.column] = resolvedChildValue;
        }
      });
    });

    sectionDef.rows.forEach((rowDef) => {
      if (record[rowDef.key] === undefined) record[rowDef.key] = null;
      (rowDef.children || []).forEach((child) => {
        if (record[child.column] === undefined) record[child.column] = null;
      });
    });

    return record;
  }).filter((row) => row.period && row.period_numeric);
};

const upsertSectionRows = async (sectionKey, master, snapshot, activeStockId, db = pool) => {
  const sectionDef = SPLIT_TABLES[sectionKey];
  if (!sectionDef) {
    throw new Error(`Unknown split section: ${sectionKey}`);
  }

  const periods = buildPeriodRows(sectionKey, snapshot, master, activeStockId);
  if (!periods.length) {
    return { rows: [], count: 0 };
  }

  const columns = buildSectionColumns(sectionDef);
  const updateColumns = columns.filter((column) => !["master_id", "period_numeric", "created_at"].includes(column));
  const values = [];
  const placeholders = periods.map((row, rowIndex) => {
    const offset = rowIndex * columns.length;
    const rowPlaceholders = columns.map((_, colIndex) => `$${offset + colIndex + 1}`);
    values.push(
      row.master_id,
      row.active_stock_id,
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
      ...sectionDef.rows.flatMap((rowDef) => {
        const out = [row[rowDef.key] ?? null];
        (rowDef.children || []).forEach((child) => out.push(row[child.column] ?? null));
        return out;
      }),
      row.last_updated_at || new Date(),
      row.updated_at || new Date(),
    );
    return `(${rowPlaceholders.join(", ")})`;
  });

  const updateSetClause = updateColumns.map((column) => `${column} = EXCLUDED.${column}`).join(", ");

  const { rows } = await db.query(
    `
      INSERT INTO ${sectionDef.tableName} (
        ${columns.join(", ")}
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

const getByMasterId = async (sectionKey, masterId, db = pool) => {
  const sectionDef = SPLIT_TABLES[sectionKey];
  if (!sectionDef) return [];

  const { rows } = await db.query(
    `
      SELECT *
      FROM ${sectionDef.tableName}
      WHERE master_id = $1
      ORDER BY period_numeric ASC, id ASC
    `,
    [Number(masterId)],
  );
  return rows;
};

const getBySymbol = async (sectionKey, symbol, db = pool) => {
  const sectionDef = SPLIT_TABLES[sectionKey];
  if (!sectionDef) return [];

  const { rows } = await db.query(
    `
      SELECT t.*
      FROM ${sectionDef.tableName} t
      INNER JOIN stock_master sm ON sm.id = t.master_id
      WHERE sm.symbol = $1
      ORDER BY t.period_numeric ASC, t.id ASC
    `,
    [String(symbol || "").trim()],
  );
  return rows;
};

module.exports = {
  SPLIT_TABLES,
  ensureActiveStockId,
  buildPeriodRows,
  upsertSectionRows,
  getByMasterId,
  getBySymbol,
};
