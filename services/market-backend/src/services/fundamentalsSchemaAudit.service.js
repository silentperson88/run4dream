const fs = require("fs/promises");
const path = require("path");
const {
  cleanLabel,
  normalizeLabel,
  fundamentalsMapping,
} = require("../config/fundamentalsRow.mapping");
const { NORMALIZED_FUNDAMENTALS_SCHEMA } = require("../config/fundamentalsNormalized.schema");

const AUDIT_FILE_PATH = path.join(
  __dirname,
  "../tmp/fundamentals/schema-audit.json",
);

const AUDIT_FILE_DIR = path.dirname(AUDIT_FILE_PATH);
const FINAL_SCHEMA_FILE_PATH = path.join(
  __dirname,
  "../tmp/fundamentals/final-schema.json",
);
const FINAL_SCHEMA_FILE_DIR = path.dirname(FINAL_SCHEMA_FILE_PATH);
const FINAL_SCHEMA_SQL_FILE_PATH = path.join(
  __dirname,
  "../tmp/fundamentals/final-schema.sql",
);
const FINAL_SCHEMA_SQL_FILE_DIR = path.dirname(FINAL_SCHEMA_SQL_FILE_PATH);
const EXAMPLE_LIMIT = 5;
const FINAL_SCHEMA_ALLOWED_TABLES = new Set([
  "company_overview",
  "quarterly_results",
  "profit_loss",
  "balance_sheet",
  "cash_flow",
  "ratios",
  "shareholdings",
]);

const FINAL_SCHEMA_JSON_COLUMNS = new Set([
  "pros",
  "cons",
  "links",
  "source_payload",
  "headers",
  "raw_row",
  "children",
]);

const FINAL_SCHEMA_TEXT_COLUMNS = new Set([
  "company_name",
  "about",
  "key_points",
  "high_low",
  "row_label",
  "title",
  "raw_pdf",
  "other_income_normal",
  "sales_growth_percent",
  "yoy_sales_growth_percent",
  "yoy_profit_growth_percent",
  "profit_growth_percent",
  "dividend_payout_percent",
  "exceptional_items",
  "profit_excl_excep",
  "exceptional_items_at",
  "profit_for_eps",
  "profit_for_pe",
  "minority_share",
  "material_cost_percent",
  "employee_cost_percent",
  "manufacturing_cost_percent",
  "other_cost_percent",
  "csg_10y",
  "csg_5y",
  "csg_3y",
  "csg_ttm",
  "cpg_10y",
  "cpg_5y",
  "cpg_3y",
  "cpg_ttm",
  "spc_10y",
  "spc_5y",
  "spc_3y",
  "spc_1y",
  "roe_10y",
  "roe_5y",
  "roe_3y",
  "roe_last_year",
]);

const normalizeColumnKey = (label) => normalizeKey(label || "").replace(/\s+/g, "_");

const columnDef = (key, label, kind = "field", aliases = [], sourceAliases = []) => ({
  key,
  label,
  aliases,
  source_aliases: sourceAliases,
  kind,
});

const flattenRowDefs = (rows = []) => {
  const columns = [];
  const seen = new Set();

  const pushColumn = (key, label, kind, aliases = [], sourceAliases = []) => {
    const normalizedKey = String(key || "").trim();
    if (!normalizedKey || seen.has(normalizedKey)) return;
    seen.add(normalizedKey);
    columns.push(columnDef(normalizedKey, label || normalizedKey, kind, aliases, sourceAliases));
  };

  rows.forEach((row) => {
    if (!row?.key) return;
    pushColumn(row.key, row.label || row.key, "field", row.aliases || [], row.sourceAliases || []);
    (row.children || []).forEach((childLabel) => {
      if (!childLabel || childLabel === "<dynamic_holder_rows>") return;
      pushColumn(
        normalizeColumnKey(childLabel),
        childLabel,
        "child",
        [],
        [childLabel],
      );
    });
  });

  return columns;
};

const getFixedFinalColumns = (tableKey) => {
  if (tableKey === "company_overview") {
    const overview = NORMALIZED_FUNDAMENTALS_SCHEMA?.overview || {};
    return [
      ...flattenRowDefs(overview.fields || []),
      ...flattenRowDefs(overview.top_ratios || []),
      columnDef("pros", "Pros", "json"),
      columnDef("cons", "Cons", "json"),
      columnDef("links", "Links", "json"),
      columnDef("source_payload", "Source Payload", "json"),
    ];
  }

  if (tableKey === "quarterly_results") {
    return flattenRowDefs(NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.quarterly_results?.rowKeys || []);
  }

  if (tableKey === "profit_loss") {
    const section = NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.profit_loss || {};
    return [
      ...flattenRowDefs(section.extraFields || []),
      ...flattenRowDefs(section.rowKeys || []),
    ];
  }

  if (tableKey === "balance_sheet") {
    return flattenRowDefs(NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.balance_sheet?.rowKeys || []);
  }

  if (tableKey === "cash_flow") {
    return flattenRowDefs(NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.cash_flow?.rowKeys || []);
  }

  if (tableKey === "ratios") {
    return flattenRowDefs(NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.ratios?.rowKeys || []);
  }

  if (tableKey === "shareholdings") {
    return [
      ...flattenRowDefs(NORMALIZED_FUNDAMENTALS_SCHEMA?.sections?.shareholdings?.rowKeys || []),
      columnDef("children", "Children", "json"),
    ];
  }

  return [];
};

const normalizeKey = (value) => normalizeLabel(cleanLabel(value || ""));

const firstNonEmpty = (...values) =>
  values.find((value) => value !== null && value !== undefined && String(value).trim() !== "") ??
  null;

const ensureArray = (value) => (Array.isArray(value) ? value : []);

const ensureObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

const getByPath = (obj, pathExpression) => {
  if (!obj || !pathExpression) return null;
  return String(pathExpression)
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
  return String(value).trim() || null;
};

const coerceNumeric = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const text = String(value)
    .replace(/,/g, "")
    .replace(/â‚¹/g, "")
    .replace(/Rs\.?/gi, "")
    .replace(/Cr\.?/gi, "")
    .replace(/%/g, "")
    .trim();
  if (!text || ["na", "n/a", "-", "--"].includes(text.toLowerCase())) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildExample = (snapshot, sectionKey, sourceLabel, rowType, extra = {}) => ({
  master_id: snapshot?.master_id ? String(snapshot.master_id) : null,
  company: snapshot?.company || null,
  section: sectionKey,
  source_label: sourceLabel || null,
  row_type: rowType || null,
  ...extra,
});

const mergeExampleList = (existing = [], incoming = []) => {
  const seen = new Set(existing.map((item) => JSON.stringify(item)));
  const merged = [...existing];
  for (const item of incoming) {
    const fingerprint = JSON.stringify(item);
    if (seen.has(fingerprint)) continue;
    seen.add(fingerprint);
    merged.push(item);
    if (merged.length >= EXAMPLE_LIMIT) break;
  }
  return merged.slice(0, EXAMPLE_LIMIT);
};

const createEmptyBucket = ({ key, label, table, kind }) => ({
  key,
  label,
  table,
  kind,
  planned_columns: [],
  matched_columns: {},
  unmatched_rows: {},
  observed_child_rows: {},
  matched_count: 0,
  unmatched_count: 0,
  total_rows_seen: 0,
});

const createAuditState = () => ({
  generated_at: null,
  updated_at: null,
  totals: {
    snapshots_scanned: 0,
    matched_rows: 0,
    unmatched_rows: 0,
  },
  tables: {
    company_overview: createEmptyBucket({
      key: "company_overview",
      label: "Company Overview",
      table: "stock_fundamental_overview",
      kind: "overview",
    }),
    quarterly_results: createEmptyBucket({
      key: "quarterly_results",
      label: "Quarterly Results",
      table: "stock_fundamental_quarterly_results",
      kind: "period_table",
    }),
    profit_loss: createEmptyBucket({
      key: "profit_loss",
      label: "Profit & Loss",
      table: "stock_fundamental_profit_loss_periods",
      kind: "period_table",
    }),
    balance_sheet: createEmptyBucket({
      key: "balance_sheet",
      label: "Balance Sheet",
      table: "stock_fundamental_balance_sheet_periods",
      kind: "period_table",
    }),
    cash_flow: createEmptyBucket({
      key: "cash_flow",
      label: "Cash Flows",
      table: "stock_fundamental_cash_flow_periods",
      kind: "period_table",
    }),
    ratios: createEmptyBucket({
      key: "ratios",
      label: "Ratios",
      table: "stock_fundamental_ratios_periods",
      kind: "period_table",
    }),
    shareholdings: createEmptyBucket({
      key: "shareholdings",
      label: "Shareholding Pattern",
      table: "stock_fundamental_shareholdings_periods",
      kind: "mixed_table",
    }),
  },
});

const mergeBucketMap = (target, incoming) => {
  const out = target || {};
  Object.entries(incoming || {}).forEach(([key, value]) => {
    if (!out[key]) {
      out[key] = { ...value };
      return;
    }

    const current = out[key];
    current.planned_columns = value.planned_columns?.length
      ? value.planned_columns
      : current.planned_columns;
    current.total_rows_seen += value.total_rows_seen || 0;
    current.matched_count += value.matched_count || 0;
    current.unmatched_count += value.unmatched_count || 0;

    Object.entries(value.matched_columns || {}).forEach(([columnKey, columnValue]) => {
      if (!current.matched_columns[columnKey]) {
        current.matched_columns[columnKey] = { ...columnValue };
        return;
      }
      const existing = current.matched_columns[columnKey];
      existing.count = (existing.count || 0) + (columnValue.count || 0);
      existing.stock_count = (existing.stock_count || 0) + (columnValue.stock_count || 0);
      existing.examples = mergeExampleList(existing.examples || [], columnValue.examples || []);
      existing.source_labels = Array.from(
        new Set([...(existing.source_labels || []), ...(columnValue.source_labels || [])]),
      );
    });

    Object.entries(value.unmatched_rows || {}).forEach(([labelKey, rowValue]) => {
      if (!current.unmatched_rows[labelKey]) {
        current.unmatched_rows[labelKey] = { ...rowValue };
        return;
      }
      const existing = current.unmatched_rows[labelKey];
      existing.count = (existing.count || 0) + (rowValue.count || 0);
      existing.stock_count = (existing.stock_count || 0) + (rowValue.stock_count || 0);
      existing.examples = mergeExampleList(existing.examples || [], rowValue.examples || []);
      existing.source_labels = Array.from(
        new Set([...(existing.source_labels || []), ...(rowValue.source_labels || [])]),
      );
    });

    Object.entries(value.observed_child_rows || {}).forEach(([labelKey, childValue]) => {
      if (!current.observed_child_rows[labelKey]) {
        current.observed_child_rows[labelKey] = { ...childValue };
        return;
      }
      const existing = current.observed_child_rows[labelKey];
      existing.count = (existing.count || 0) + (childValue.count || 0);
      existing.stock_count = (existing.stock_count || 0) + (childValue.stock_count || 0);
      existing.examples = mergeExampleList(existing.examples || [], childValue.examples || []);
    });
  });
  return out;
};

const loadAudit = async () => {
  try {
    const raw = await fs.readFile(AUDIT_FILE_PATH, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    return createAuditState();
  }
};

const saveAudit = async (audit) => {
  await fs.mkdir(AUDIT_FILE_DIR, { recursive: true });
  await fs.writeFile(AUDIT_FILE_PATH, JSON.stringify(audit, null, 2), "utf8");
  return AUDIT_FILE_PATH;
};

const buildLabelIndex = (rowsConfig = []) => {
  const index = new Map();
  rowsConfig.forEach((cfg) => {
    if (!cfg?.key) return;
    const labels = [cfg.label, ...(cfg.aliases || [])];
    labels.forEach((label) => {
      const normalized = normalizeKey(label);
      if (normalized) index.set(normalized, cfg);
    });
  });
  return index;
};

const getRowLabel = (row, headers = []) => {
  if (!row || typeof row !== "object") return null;
  if (typeof row.label === "string" && row.label.trim()) return row.label.trim();
  if (typeof row.title === "string" && row.title.trim()) return row.title.trim();
  if (headers.length && typeof row[headers[0]] === "string") {
    const value = row[headers[0]].trim();
    return value || null;
  }
  return null;
};

const captureMatchedColumn = (bucket, cfg, snapshot, sectionKey, sourceLabel, extra = {}) => {
  const columnKey = cfg.key;
  if (!bucket.matched_columns[columnKey]) {
    bucket.matched_columns[columnKey] = {
      key: columnKey,
      label: cfg.label || columnKey,
      aliases: cfg.aliases || [],
      source_aliases: cfg.sourceAliases || [],
      count: 0,
      stock_count: 0,
      source_labels: [],
      examples: [],
      extra,
    };
  }

  const entry = bucket.matched_columns[columnKey];
  entry.count += 1;
  entry.stock_count += 1;
  entry.source_labels = Array.from(new Set([...(entry.source_labels || []), sourceLabel]));
  entry.examples = mergeExampleList(entry.examples || [], [
    buildExample(snapshot, sectionKey, sourceLabel, "matched", extra),
  ]);

  bucket.matched_count += 1;
  bucket.total_rows_seen += 1;
};

const captureUnmatchedRow = (bucket, snapshot, sectionKey, sourceLabel, extra = {}) => {
  const labelKey = normalizeKey(sourceLabel || "unknown");
  if (!bucket.unmatched_rows[labelKey]) {
    bucket.unmatched_rows[labelKey] = {
      label: sourceLabel || null,
      count: 0,
      stock_count: 0,
      source_labels: [],
      examples: [],
      extra,
    };
  }

  const entry = bucket.unmatched_rows[labelKey];
  entry.count += 1;
  entry.stock_count += 1;
  entry.source_labels = Array.from(new Set([...(entry.source_labels || []), sourceLabel]));
  entry.examples = mergeExampleList(entry.examples || [], [
    buildExample(snapshot, sectionKey, sourceLabel, "unmatched", extra),
  ]);

  bucket.unmatched_count += 1;
  bucket.total_rows_seen += 1;
};

const captureChildRow = (bucket, snapshot, sectionKey, parentLabel, childLabel, matched) => {
  const labelKey = normalizeKey(childLabel || "unknown");
  if (!bucket.observed_child_rows[labelKey]) {
    bucket.observed_child_rows[labelKey] = {
      label: childLabel || null,
      parent_label: parentLabel || null,
      count: 0,
      stock_count: 0,
      examples: [],
      matched,
    };
  }

  const entry = bucket.observed_child_rows[labelKey];
  entry.count += 1;
  entry.stock_count += 1;
  entry.matched = entry.matched || matched;
  entry.examples = mergeExampleList(entry.examples || [], [
    buildExample(snapshot, sectionKey, childLabel, matched ? "matched_child" : "unmatched_child", {
      parent_label: parentLabel || null,
    }),
  ]);
};

const buildOverviewBucket = (snapshot) => {
  const bucket = createEmptyBucket({
    key: "company_overview",
    label: "Company Overview",
    table: "stock_fundamental_overview",
    kind: "overview",
  });
  const overviewFields = ensureArray(NORMALIZED_FUNDAMENTALS_SCHEMA?.overview?.fields);
  const companyInfo = ensureObject(snapshot?.company_info);
  const summary = ensureObject(snapshot?.summary);
  const marketSnapshot = ensureObject(summary?.market_snapshot);
  const otherDetails = ensureObject(snapshot?.other_details);
  const profitLossDetails = ensureObject(otherDetails?.profit_loss);

  overviewFields.forEach((field) => {
    const candidatePaths = ensureArray(field?.sourceAliases);
    const value = firstNonEmpty(...candidatePaths.map((path) => getByPath(snapshot, path)));
    const hasValue = value !== null && value !== undefined && String(value).trim() !== "";
    if (!bucket.matched_columns[field.key]) {
      bucket.matched_columns[field.key] = {
        key: field.key,
        label: field.label || field.key,
        aliases: field.aliases || [],
        source_aliases: candidatePaths,
        count: 0,
        stock_count: 0,
        source_labels: [],
        examples: [],
      };
    }
    const entry = bucket.matched_columns[field.key];
    entry.count += 1;
    entry.stock_count += 1;
    entry.examples = mergeExampleList(entry.examples || [], [
      buildExample(snapshot, "company_overview", field.label || field.key, hasValue ? "matched" : "unmatched", {
        source_paths: candidatePaths,
      }),
    ]);
    if (hasValue) {
      entry.source_labels = Array.from(new Set([...(entry.source_labels || []), field.label || field.key]));
      bucket.matched_count += 1;
    } else {
      bucket.unmatched_rows[normalizeKey(field.label || field.key)] = {
        label: field.label || field.key,
        count: 1,
        stock_count: 1,
        source_labels: [field.label || field.key],
        examples: [
          buildExample(snapshot, "company_overview", field.label || field.key, "unmatched", {
            source_paths: candidatePaths,
          }),
        ],
      };
      bucket.unmatched_count += 1;
    }
    bucket.total_rows_seen += 1;
  });

  // Add top-level extra overview arrays as columns with JSON storage intent.
  [
    ["pros", summary?.pros],
    ["cons", summary?.cons],
    ["links", companyInfo?.links],
  ].forEach(([key, value]) => {
    const hasValue = Array.isArray(value) ? value.length > 0 : value !== null && value !== undefined;
    bucket.matched_columns[key] = {
      key,
      label: key,
      aliases: [],
      source_aliases: [],
      count: 1,
      stock_count: 1,
      source_labels: [key],
      examples: [
        buildExample(snapshot, "company_overview", key, hasValue ? "matched" : "unmatched", {
          value_type: Array.isArray(value) ? "array" : typeof value,
        }),
      ],
    };
    if (hasValue) bucket.matched_count += 1;
    else {
      bucket.unmatched_rows[normalizeKey(key)] = {
        label: key,
        count: 1,
        stock_count: 1,
        source_labels: [key],
        examples: [
          buildExample(snapshot, "company_overview", key, "unmatched", {
            value_type: Array.isArray(value) ? "array" : typeof value,
          }),
        ],
      };
      bucket.unmatched_count += 1;
    }
    bucket.total_rows_seen += 1;
  });

  // Preserve the rest of the overview data as a compact payload for review.
  bucket.planned_columns = overviewFields.map((field) => ({
    key: field.key,
    label: field.label,
    aliases: field.aliases || [],
    source_aliases: field.sourceAliases || [],
  }));
  bucket.source_payload = {
    company_info: {
      company_name: companyInfo?.company_name || null,
      about: companyInfo?.about || null,
      key_points: companyInfo?.key_points || null,
      links: companyInfo?.links || [],
    },
    summary: {
      market_snapshot: marketSnapshot,
      pros: ensureArray(summary?.pros),
      cons: ensureArray(summary?.cons),
    },
    other_details: otherDetails,
  };

  return bucket;
};

const buildSectionBucket = (snapshot, sectionKey, sectionConfig = {}, sourceTableKey) => {
  const bucket = createEmptyBucket({
    key: sectionKey,
    label: sectionConfig?.label || sectionKey,
    table: sectionConfig?.table || sourceTableKey,
    kind: sectionKey === "shareholdings" ? "mixed_table" : "period_table",
  });

  const rowsConfig = ensureArray(sectionConfig?.rows);
  const extraFields = ensureArray(sectionConfig?.extraFields);
  const labelIndex = buildLabelIndex(rowsConfig);
  bucket.planned_columns = [
    ...rowsConfig.map((cfg) => ({
      key: cfg.key,
      label: cfg.label,
      aliases: cfg.aliases || [],
    })),
    ...extraFields.map((cfg) => ({
      key: cfg.key,
      label: cfg.label,
      aliases: cfg.aliases || [],
      source_aliases: cfg.sourceAliases || [],
      kind: "extra_field",
    })),
  ];

  extraFields.forEach((cfg) => {
    const value = firstNonEmpty(...ensureArray(cfg?.sourceAliases).map((path) => getByPath(snapshot, path)));
    const hasValue = value !== null && value !== undefined && String(value).trim() !== "";
    if (!bucket.matched_columns[cfg.key]) {
      bucket.matched_columns[cfg.key] = {
        key: cfg.key,
        label: cfg.label || cfg.key,
        aliases: cfg.aliases || [],
        source_aliases: cfg.sourceAliases || [],
        count: 0,
        stock_count: 0,
        source_labels: [],
        examples: [],
        kind: "extra_field",
      };
    }
    const entry = bucket.matched_columns[cfg.key];
    entry.count += 1;
    entry.stock_count += 1;
    entry.examples = mergeExampleList(entry.examples || [], [
      buildExample(snapshot, sectionKey, cfg.label || cfg.key, hasValue ? "matched" : "unmatched", {
        source_paths: cfg.sourceAliases || [],
        kind: "extra_field",
      }),
    ]);
    if (hasValue) {
      entry.source_labels = Array.from(
        new Set([...(entry.source_labels || []), ...(cfg.sourceAliases || [])]),
      );
      bucket.matched_count += 1;
    } else {
      bucket.unmatched_rows[normalizeKey(cfg.label || cfg.key)] = {
        label: cfg.label || cfg.key,
        count: 1,
        stock_count: 1,
        source_labels: cfg.sourceAliases || [],
        examples: [
          buildExample(snapshot, sectionKey, cfg.label || cfg.key, "unmatched", {
            source_paths: cfg.sourceAliases || [],
            kind: "extra_field",
          }),
        ],
      };
      bucket.unmatched_count += 1;
    }
    bucket.total_rows_seen += 1;
  });

  const tableNode = ensureObject(snapshot?.[sourceTableKey]);
  const headers = ensureArray(tableNode?.headers);
  const rows = ensureObject(tableNode?.rows);

  Object.entries(rows).forEach(([rowKey, row]) => {
    const rowLabel = getRowLabel(row, headers);
    if (rowLabel) {
      const cfg = labelIndex.get(normalizeKey(rowLabel));
      if (cfg) {
        captureMatchedColumn(bucket, cfg, snapshot, sectionKey, rowLabel, { row_key: rowKey, row_type: "parent" });
      } else {
        captureUnmatchedRow(bucket, snapshot, sectionKey, rowLabel, { row_key: rowKey, row_type: "parent" });
      }
    }

    const children = ensureArray(row?.children);
    children.forEach((child, childIndex) => {
      const childLabel = getRowLabel(child, headers);
      if (!childLabel) return;
      const cfg = labelIndex.get(normalizeKey(childLabel));
      const matched = Boolean(cfg);

      if (sectionKey === "shareholdings") {
        captureChildRow(bucket, snapshot, sectionKey, rowLabel, childLabel, matched);
        if (!matched) {
          bucket.unmatched_rows[normalizeKey(childLabel)] = {
            label: childLabel,
            count: (bucket.unmatched_rows[normalizeKey(childLabel)]?.count || 0) + 1,
            stock_count: (bucket.unmatched_rows[normalizeKey(childLabel)]?.stock_count || 0) + 1,
            source_labels: Array.from(
              new Set([
                ...ensureArray(bucket.unmatched_rows[normalizeKey(childLabel)]?.source_labels || []),
                childLabel,
              ]),
            ),
            examples: mergeExampleList(
              ensureArray(bucket.unmatched_rows[normalizeKey(childLabel)]?.examples || []),
              [
                buildExample(snapshot, sectionKey, childLabel, "unmatched_child", {
                  parent_label: rowLabel || null,
                  child_index: childIndex,
                }),
              ],
            ),
          };
        }
        if (matched) {
          bucket.matched_count += 1;
          bucket.total_rows_seen += 1;
        } else {
          bucket.unmatched_count += 1;
          bucket.total_rows_seen += 1;
        }
        return;
      }

      if (matched) {
        captureMatchedColumn(bucket, cfg, snapshot, sectionKey, childLabel, {
          parent_label: rowLabel || null,
          child_index: childIndex,
          row_key: rowKey,
          row_type: "child",
        });
      } else {
        captureUnmatchedRow(bucket, snapshot, sectionKey, childLabel, {
          parent_label: rowLabel || null,
          child_index: childIndex,
          row_key: rowKey,
          row_type: "child",
        });
      }
    });
  });

  return bucket;
};

const buildAuditForSnapshot = (snapshot) => {
  const tables = {};
  tables.company_overview = buildOverviewBucket(snapshot);
  tables.quarterly_results = buildSectionBucket(
    snapshot,
    "quarterly_results",
    {
      label: "Quarterly Results",
      table: "stock_fundamental_quarterly_results",
      rows: ensureArray(fundamentalsMapping?.quarters?.rows),
    },
    "quarters_table",
  );
  tables.profit_loss = buildSectionBucket(
    snapshot,
    "profit_loss",
    {
      label: "Profit & Loss",
      table: "stock_fundamental_profit_loss_periods",
      rows: [
        ...ensureArray(fundamentalsMapping?.profit_loss?.rows),
      ],
      extraFields: [
        { key: "csg_10y", label: "10 Years:", sourceAliases: ["other_details.profit_loss.compounded_sales_growth.entries.0.value"] },
        { key: "csg_5y", label: "5 Years:", sourceAliases: ["other_details.profit_loss.compounded_sales_growth.entries.1.value"] },
        { key: "csg_3y", label: "3 Years:", sourceAliases: ["other_details.profit_loss.compounded_sales_growth.entries.2.value"] },
        { key: "csg_ttm", label: "TTM:", sourceAliases: ["other_details.profit_loss.compounded_sales_growth.entries.3.value"] },
        { key: "cpg_10y", label: "10 Years:", sourceAliases: ["other_details.profit_loss.compounded_profit_growth.entries.0.value"] },
        { key: "cpg_5y", label: "5 Years:", sourceAliases: ["other_details.profit_loss.compounded_profit_growth.entries.1.value"] },
        { key: "cpg_3y", label: "3 Years:", sourceAliases: ["other_details.profit_loss.compounded_profit_growth.entries.2.value"] },
        { key: "cpg_ttm", label: "TTM:", sourceAliases: ["other_details.profit_loss.compounded_profit_growth.entries.3.value"] },
        { key: "spc_10y", label: "10 Years:", sourceAliases: ["other_details.profit_loss.stock_price_cagr.entries.0.value"] },
        { key: "spc_5y", label: "5 Years:", sourceAliases: ["other_details.profit_loss.stock_price_cagr.entries.1.value"] },
        { key: "spc_3y", label: "3 Years:", sourceAliases: ["other_details.profit_loss.stock_price_cagr.entries.2.value"] },
        { key: "spc_1y", label: "1 Year:", sourceAliases: ["other_details.profit_loss.stock_price_cagr.entries.3.value"] },
        { key: "roe_10y", label: "10 Years:", sourceAliases: ["other_details.profit_loss.return_on_equity.entries.0.value"] },
        { key: "roe_5y", label: "5 Years:", sourceAliases: ["other_details.profit_loss.return_on_equity.entries.1.value"] },
        { key: "roe_3y", label: "3 Years:", sourceAliases: ["other_details.profit_loss.return_on_equity.entries.2.value"] },
        { key: "roe_last_year", label: "Last Year:", sourceAliases: ["other_details.profit_loss.return_on_equity.entries.3.value"] },
      ],
    },
    "profit_loss_table",
  );
  tables.balance_sheet = buildSectionBucket(
    snapshot,
    "balance_sheet",
    {
      label: "Balance Sheet",
      table: "stock_fundamental_balance_sheet_periods",
      rows: ensureArray(fundamentalsMapping?.balance_sheet?.rows),
    },
    "balance_sheet_table",
  );
  tables.cash_flow = buildSectionBucket(
    snapshot,
    "cash_flow",
    {
      label: "Cash Flows",
      table: "stock_fundamental_cash_flow_periods",
      rows: ensureArray(fundamentalsMapping?.cash_flow?.rows),
    },
    "cash_flow_table",
  );
  tables.ratios = buildSectionBucket(
    snapshot,
    "ratios",
    {
      label: "Ratios",
      table: "stock_fundamental_ratios_periods",
      rows: ensureArray(fundamentalsMapping?.ratios?.rows),
    },
    "ratios_table",
  );
  tables.shareholdings = buildSectionBucket(
    snapshot,
    "shareholdings",
    {
      label: "Shareholding Pattern",
      table: "stock_fundamental_shareholdings_periods",
      rows: ensureArray(fundamentalsMapping?.shareholdings?.rows),
    },
    "shareholdings_table",
  );

  return tables;
};

const mergeAudit = (baseAudit, nextAudit) => {
  const audit = baseAudit || createAuditState();
  audit.generated_at = audit.generated_at || new Date().toISOString();
  audit.updated_at = new Date().toISOString();
  audit.totals.snapshots_scanned =
    (audit.totals.snapshots_scanned || 0) + (nextAudit.totals.snapshots_scanned || 0);
  audit.totals.matched_rows =
    (audit.totals.matched_rows || 0) + (nextAudit.totals.matched_rows || 0);
  audit.totals.unmatched_rows =
    (audit.totals.unmatched_rows || 0) + (nextAudit.totals.unmatched_rows || 0);
  audit.tables = mergeBucketMap(audit.tables || {}, nextAudit.tables || {});
  return audit;
};

const finalizeBucket = (bucket) => ({
  ...bucket,
  planned_columns: bucket.planned_columns || [],
  matched_columns: bucket.matched_columns || {},
  unmatched_rows: bucket.unmatched_rows || {},
  observed_child_rows: bucket.observed_child_rows || {},
});

const finalizeAudit = (audit) => {
  const out = ensureObject(audit);
  out.tables = Object.fromEntries(
    Object.entries(ensureObject(out.tables)).map(([key, bucket]) => [
      key,
      finalizeBucket(bucket),
    ]),
  );
  return out;
};

const buildFinalSchemaFromSelection = (audit, selection = {}) => {
  const finalizedTables = {};
  const auditTables = ensureObject(audit?.tables);
  Object.entries(auditTables).forEach(([tableKey, bucket]) => {
    if (!FINAL_SCHEMA_ALLOWED_TABLES.has(tableKey)) return;
    const selectedColumns = getFixedFinalColumns(tableKey);

    finalizedTables[tableKey] = {
      key: bucket.key,
      label: bucket.label,
      table: bucket.table,
      kind: bucket.kind,
      selected_columns: selectedColumns,
      ignored_unmatched_rows: Object.values(bucket.unmatched_rows || {}).map((row) => ({
        label: row.label || null,
        source_labels: row.source_labels || [],
        examples: row.examples || [],
      })),
      observed_child_rows:
        tableKey === "shareholdings"
          ? Object.values(bucket.observed_child_rows || {}).map((row) => ({
              label: row.label || null,
              parent_label: row.parent_label || null,
              matched: Boolean(row.matched),
              examples: row.examples || [],
            }))
          : [],
    };
  });

  return {
    generated_at: new Date().toISOString(),
    source_audit_file: AUDIT_FILE_PATH,
    tables: finalizedTables,
  };
};

const saveFinalSchema = async (schema) => {
  await fs.mkdir(FINAL_SCHEMA_FILE_DIR, { recursive: true });
  await fs.writeFile(FINAL_SCHEMA_FILE_PATH, JSON.stringify(schema, null, 2), "utf8");
  return FINAL_SCHEMA_FILE_PATH;
};

const inferFinalSchemaColumnType = (tableKey, column) => {
  const key = String(column?.key || "").toLowerCase();
  if (!key) return "TEXT";

  if (FINAL_SCHEMA_JSON_COLUMNS.has(key)) return "JSONB";
  if (tableKey === "company_overview") {
    if (FINAL_SCHEMA_TEXT_COLUMNS.has(key)) return "TEXT";
    return "NUMERIC";
  }

  if (tableKey === "shareholdings") {
    if (key === "period_label") return "TEXT";
    if (key === "period_end") return "DATE";
    if (key === "period_index") return "INTEGER";
    if (FINAL_SCHEMA_TEXT_COLUMNS.has(key)) return "TEXT";
    return "NUMERIC";
  }

  if (key === "period_label") return "TEXT";
  if (key === "period_end") return "DATE";
  if (key === "period_index") return "INTEGER";
  if (FINAL_SCHEMA_TEXT_COLUMNS.has(key)) return "TEXT";

  return "NUMERIC";
};

const buildFinalSchemaSql = (schema) => {
  const tables = ensureObject(schema?.tables);
  const statements = [
    "-- Auto-generated from the Fundamentals Schema Audit finalize step.",
    "-- Review before applying if your database already has the structured tables.",
    "",
  ];

  Object.entries(tables).forEach(([tableKey, bucket]) => {
    if (!FINAL_SCHEMA_ALLOWED_TABLES.has(tableKey)) return;
    const tableName = bucket?.table;
    const selectedColumns = ensureArray(bucket?.selected_columns);
    if (!tableName) return;

    const constraintBase = String(tableName).replace(/[^a-zA-Z0-9_]+/g, "_");
    const uniqueConstraintName =
      tableKey === "company_overview"
        ? `uq_${constraintBase}_master_id`
        : `uq_${constraintBase}_master_period`;

    statements.push(`DROP TABLE IF EXISTS ${tableName} CASCADE;`);
    statements.push("");
    const columnLines = [
      "  id BIGSERIAL PRIMARY KEY",
      "  master_id BIGINT NOT NULL",
      "  active_stock_id BIGINT NULL",
      "  snapshot_id BIGINT NULL",
    ];

    if (tableKey !== "company_overview") {
      columnLines.push("  period_label TEXT NULL");
      columnLines.push("  period_end DATE NULL");
      columnLines.push("  period_index INTEGER NULL");
      columnLines.push("  title TEXT NULL");
      columnLines.push("  headers JSONB NULL");
      columnLines.push("  raw_row JSONB NULL");
      columnLines.push("  row_label TEXT NULL");
    }

    selectedColumns.forEach((column) => {
      const sqlType = inferFinalSchemaColumnType(tableKey, column);
      columnLines.push(`  ${column.key} ${sqlType} NULL`);
    });

    columnLines.push("  last_updated_at TIMESTAMPTZ NULL");
    columnLines.push("  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()");
    columnLines.push("  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()");

    const uniqueConstraint =
      tableKey === "company_overview"
        ? `  CONSTRAINT ${uniqueConstraintName} UNIQUE (master_id)`
        : `  CONSTRAINT ${uniqueConstraintName} UNIQUE (master_id, period_label)`;

    statements.push(`CREATE TABLE IF NOT EXISTS ${tableName} (`);
    statements.push(columnLines.join(",\n"));
    statements.push(`,\n${uniqueConstraint}`);
    statements.push(");");
    statements.push("");
  });

  return statements.join("\n");
};

const saveFinalSchemaSql = async (schema) => {
  await fs.mkdir(FINAL_SCHEMA_SQL_FILE_DIR, { recursive: true });
  const sql = buildFinalSchemaSql(schema);
  await fs.writeFile(FINAL_SCHEMA_SQL_FILE_PATH, sql, "utf8");
  return {
    filePath: FINAL_SCHEMA_SQL_FILE_PATH,
    sql,
  };
};

module.exports = {
  AUDIT_FILE_PATH,
  FINAL_SCHEMA_FILE_PATH,
  FINAL_SCHEMA_SQL_FILE_PATH,
  createAuditState,
  loadAudit,
  saveAudit,
  buildAuditForSnapshot,
  mergeAudit,
  finalizeAudit,
  buildFinalSchemaFromSelection,
  saveFinalSchema,
  buildFinalSchemaSql,
  saveFinalSchemaSql,
  normalizeKey,
  getRowLabel,
};
