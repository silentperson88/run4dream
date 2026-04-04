const { pool } = require("../config/db");
const { toNumber, syncSerialSequence } = require("./common");
const { normalizeSymbolForMatch, normalizeNameForMatch, TRAILING_SYMBOL_SUFFIX_PATTERN } = require("../utils/stockSymbolMatch");

const parseHistoryRange = (value) => {
  const historyRange = String(value ?? "").trim();
  if (!historyRange) {
    return {
      history_range: null,
      hasHistoryData: false,
      historyDataFromDate: null,
      historyDataToDate: null,
    };
  }

  const [fromDate, toDate] = historyRange.split(/\s+to\s+/i);
  return {
    history_range: historyRange,
    hasHistoryData: true,
    historyDataFromDate: String(fromDate || "").trim() || null,
    historyDataToDate: String(toDate || "").trim() || null,
  };
};

const normalizeMaster = (row = {}) => ({
  ...row,
  screener_status: row.screener_status || "PENDING",
  angelone_fetch_status: row.angelone_fetch_status || "not_attempted",
  security_code: row.security_code,
  ...parseHistoryRange(row.history_range),
});

const create = async (payload, db = pool) => {
  const params = [
    payload.symbol,
    payload.exchange,
    payload.name,
    payload.screener_url ?? "",
    payload.screener_status ?? "PENDING",
    payload.is_active ?? true,
    payload.token ?? null,
    payload.raw_stock_id ?? payload.rawStockId ?? null,
    String(payload.security_code ?? payload.securityCode ?? "").trim() || null,
    payload.history_range ?? null,
  ];

  const insertMaster = async () => {
    await syncSerialSequence(db, "stock_master", "id");
    const { rows } = await db.query(
      `
        INSERT INTO stock_master (
          symbol, exchange, name, screener_url,
          screener_status, is_active, token, raw_stock_id, security_code, history_range
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        RETURNING *
      `,
      params,
    );
    return rows[0] ? normalizeMaster(rows[0]) : null;
  };

  return insertMaster();
};

const updateById = async (id, data = {}, db = pool) => {
  const sets = [];
  const values = [Number(id)];
  const ignoredKeys = new Set([
    "company",
    "sector",
    "industry",
    "fetch_count",
    "has_history_data",
    "history_data_from_date",
    "history_data_to_date",
    "history_requested_from_date",
    "history_requested_to_date",
    "fundamentals_checked_at",
    "fundamentals_failed_fields",
    "fundamentals_failed_reason",
  ]);
  Object.entries(data).forEach(([key, value]) => {
    const col = key === "rawStockId" ? "raw_stock_id" : key;
    if (ignoredKeys.has(col)) return;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });
  if (!sets.length) return getById(id, db);

  values.push(new Date());
  sets.push(`updated_at = $${values.length}`);

  const { rows } = await db.query(
    `UPDATE stock_master SET ${sets.join(", ")} WHERE id = $1 RETURNING *`,
    values,
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getById = async (id, db = pool) => {
  const { rows } = await db.query(`SELECT * FROM stock_master WHERE id = $1 LIMIT 1`, [
    Number(id),
  ]);
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getByToken = async (token, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE token = $1 LIMIT 1`,
    [token],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getBySymbolAndExchange = async (symbol, exchange, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE symbol = $1 AND exchange = $2 LIMIT 1`,
    [String(symbol ?? "").trim(), String(exchange ?? "").trim().toUpperCase()],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getBySymbol = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE symbol = $1 LIMIT 1`,
    [String(symbol ?? "").trim()],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getByNormalizedSymbol = async (symbol, db = pool) => {
  const key = normalizeSymbolForMatch(symbol);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT *
      FROM stock_master
      WHERE regexp_replace(regexp_replace(upper(symbol), '${TRAILING_SYMBOL_SUFFIX_PATTERN}', '', 'gi'),
                           '[^A-Z0-9]+', '', 'g') = $1
      LIMIT 1
    `,
    [key],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getByName = async (name, db = pool) => {
  const key = normalizeNameForMatch(name);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT *
      FROM stock_master
      WHERE regexp_replace(lower(name), '[^a-z0-9]+', '', 'g')
        = $1
      LIMIT 1
    `,
    [key.toLowerCase()],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getBySecurityCode = async (securityCode, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE security_code = $1 LIMIT 1`,
    [String(securityCode ?? "").trim()],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getBySymbolOrName = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE symbol = $1 OR name = $1 LIMIT 1`,
    [symbol],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const listActive = async (db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE is_active = TRUE ORDER BY created_at DESC`,
  );
  return rows.map(normalizeMaster);
};

const list = async ({ page = 1, limit = 50, search = "", is_active = true } = {}, db = pool) => {
  const offset = (Number(page) - 1) * Number(limit);
  const where = [];
  const values = [];

  if (is_active !== undefined) {
    values.push(Boolean(is_active));
    where.push(`is_active = $${values.length}`);
  }

  if (search && search.trim()) {
    values.push(`%${search.trim().toLowerCase()}%`);
    where.push(`(lower(name) LIKE $${values.length} OR lower(symbol) LIKE $${values.length})`);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  values.push(Number(limit));
  const limitIdx = values.length;
  values.push(offset);
  const offsetIdx = values.length;

  const dataSql = `
    SELECT * FROM stock_master
    ${whereClause}
    ORDER BY created_at DESC
    LIMIT $${limitIdx} OFFSET $${offsetIdx}
  `;

  const countSql = `SELECT COUNT(*)::int AS total FROM stock_master ${whereClause}`;

  const [dataRes, countRes] = await Promise.all([
    db.query(dataSql, values),
    db.query(countSql, values.slice(0, where.length)),
  ]);

  return {
    data: dataRes.rows.map(normalizeMaster),
    total: Number(countRes.rows[0]?.total || 0),
    page: Number(page),
    limit: Number(limit),
  };
};

const updateHistoryCoverage = async (
  id,
  {
    actualFromDate = null,
    actualToDate = null,
  } = {},
  db = pool,
) => {
  const { rows } = await db.query(
    `
      UPDATE stock_master
      SET
        history_range = CASE
          WHEN $2::date IS NULL AND $3::date IS NULL THEN history_range
          WHEN $2::date IS NULL THEN to_char($3::date, 'YYYY-MM-DD')
          WHEN $3::date IS NULL THEN to_char($2::date, 'YYYY-MM-DD')
          ELSE to_char($2::date, 'YYYY-MM-DD') || ' to ' || to_char($3::date, 'YYYY-MM-DD')
        END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [Number(id), actualFromDate, actualToDate],
  );

  return rows[0] ? normalizeMaster(rows[0]) : null;
};

module.exports = {
  normalizeMaster,
  create,
  updateById,
  getById,
  getByToken,
  getBySymbolAndExchange,
  getBySymbol,
  getByNormalizedSymbol,
  getBySecurityCode,
  getBySymbolOrName,
  getByName,
  listActive,
  list,
  updateHistoryCoverage,
};
