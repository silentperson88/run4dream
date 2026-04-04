const { pool } = require("../config/db");
const { toNullableNumber, syncSerialSequence } = require("./common");
const { normalizeSymbolForMatch, normalizeNameForMatch, TRAILING_SYMBOL_SUFFIX_PATTERN } = require("../utils/stockSymbolMatch");

const normalizeRawStock = (row = {}) => ({
  ...row,
  exchange: row.exch_seg,
  security_code: row.security_code,
  tick_size: toNullableNumber(row.tick_size),
});

const getById = async (id, db = pool) => {
  const { rows } = await db.query(`SELECT * FROM rawstocks WHERE id = $1 LIMIT 1`, [
    Number(id),
  ]);
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const getByToken = async (token, db = pool) => {
  const { rows } = await db.query(`SELECT * FROM rawstocks WHERE token = $1 LIMIT 1`, [
    String(token ?? "").trim(),
  ]);
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const getBySymbolAndExchange = async (symbol, exchange, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM rawstocks WHERE symbol = $1 AND upper(exch_seg) = upper($2) LIMIT 1`,
    [String(symbol ?? "").trim(), String(exchange ?? "").trim()],
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const getBySymbol = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM rawstocks WHERE symbol = $1 LIMIT 1`,
    [String(symbol ?? "").trim()],
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const getByNormalizedSymbol = async (symbol, db = pool) => {
  const key = normalizeSymbolForMatch(symbol);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT *
      FROM rawstocks
      WHERE regexp_replace(regexp_replace(upper(symbol), '${TRAILING_SYMBOL_SUFFIX_PATTERN}', '', 'gi'),
                           '[^A-Z0-9]+', '', 'g') = $1
      LIMIT 1
    `,
    [key],
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const getByName = async (name, db = pool) => {
  const key = normalizeNameForMatch(name);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT *
      FROM rawstocks
      WHERE regexp_replace(lower(name), '[^a-z0-9]+', '', 'g')
        = $1
      LIMIT 1
    `,
    [key.toLowerCase()],
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const create = async (data = {}, db = pool) => {
  const token = String(data.token ?? "").trim() || null;
  const symbol = String(data.symbol ?? "").trim();
  const name = String(data.name ?? "").trim();
  const exchSeg = String(data.exchange ?? data.exch_seg ?? "").trim().toUpperCase();
  const instrumentType = String(data.instrumenttype ?? "EQ").trim().toUpperCase() || "EQ";
  const lotsize = Number(data.lotsize ?? data.lot ?? 1);
  const tickSize = toNullableNumber(data.tick_size ?? data.tickSize);

  if (!symbol || !name || !exchSeg) {
    throw new Error("symbol, name and exchange are required");
  }

  const params = [
    token,
    symbol,
    name,
    exchSeg,
    instrumentType,
    Number.isFinite(lotsize) ? lotsize : 1,
    tickSize,
    String(data.status ?? (token ? "pending" : "missing_token")).trim() || (token ? "pending" : "missing_token"),
    String(data.security_code ?? data.securityCode ?? "").trim() || null,
  ];

  const insertRawStock = async () => {
    await syncSerialSequence(db, "rawstocks", "id");
    const { rows } = await db.query(
      `
        INSERT INTO rawstocks (
          token, symbol, name, exch_seg, instrumenttype, lotsize, tick_size, status, security_code
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (token) DO UPDATE SET
          symbol = EXCLUDED.symbol,
          name = EXCLUDED.name,
          exch_seg = EXCLUDED.exch_seg,
          instrumenttype = EXCLUDED.instrumenttype,
          lotsize = EXCLUDED.lotsize,
          tick_size = EXCLUDED.tick_size,
          security_code = EXCLUDED.security_code,
          status = EXCLUDED.status
        RETURNING *
      `,
      params,
    );
    return rows[0] ? normalizeRawStock(rows[0]) : null;
  };

  return insertRawStock();
};

const updateById = async (id, data = {}, db = pool) => {
  const sets = [];
  const values = [Number(id)];

  Object.entries(data).forEach(([key, value]) => {
    const col = key === "exchange" ? "exch_seg" : key;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });

  if (!sets.length) return getById(id, db);

  const { rows } = await db.query(
    `UPDATE rawstocks SET ${sets.join(", ")} WHERE id = $1 RETURNING *`,
    values,
  );
  return rows[0] ? normalizeRawStock(rows[0]) : null;
};

const list = async (
  { page = 1, limit = 200, search = "", exchanges = [] } = {},
  db = pool,
) => {
  const offset = (Number(page) - 1) * Number(limit);
  const where = [];
  const values = [];

  if (Array.isArray(exchanges) && exchanges.length) {
    const normalizedExchanges = exchanges.map((x) => String(x).toUpperCase());
    values.push(normalizedExchanges);
    where.push(`upper(exch_seg) = ANY($${values.length})`);
  }

  if (search && search.trim()) {
    values.push(`%${search.trim().toLowerCase()}%`);
    where.push(
      `(lower(symbol) LIKE $${values.length} OR lower(name) LIKE $${values.length} OR lower(token) LIKE $${values.length})`,
    );
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  values.push(Number(limit));
  const limitIdx = values.length;
  values.push(offset);
  const offsetIdx = values.length;
  const whereValuesCount = values.length - 2;

  const dataSql = `
    SELECT *
    FROM rawstocks
    ${whereClause}
    ORDER BY symbol ASC
    LIMIT $${limitIdx} OFFSET $${offsetIdx}
  `;

  const countSql = `SELECT COUNT(*)::int AS total FROM rawstocks ${whereClause}`;

  const [dataRes, countRes] = await Promise.all([
    db.query(dataSql, values),
    db.query(countSql, values.slice(0, whereValuesCount)),
  ]);

  return {
    data: dataRes.rows.map(normalizeRawStock),
    total: Number(countRes.rows[0]?.total || 0),
    page: Number(page),
    limit: Number(limit),
  };
};

module.exports = {
  normalizeRawStock,
  getById,
  getByToken,
  getBySymbolAndExchange,
  getBySymbol,
  getByNormalizedSymbol,
  getByName,
  create,
  updateById,
  list,
};
