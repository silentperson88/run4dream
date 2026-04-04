const { pool } = require("../config/db");
const { toNumber, syncSerialSequence } = require("./common");
const { normalizeSymbolForMatch, normalizeNameForMatch, TRAILING_SYMBOL_SUFFIX_PATTERN } = require("../utils/stockSymbolMatch");

const parseHistoryRange = (value) => {
  const historyRange = String(value ?? "").trim();
  if (!historyRange) {
    return {
      historyRange: null,
      hasHistoryData: false,
      historyDataFromDate: null,
      historyDataToDate: null,
    };
  }

  const [fromDate, toDate] = historyRange.split(/\s+to\s+/i);
  return {
    historyRange,
    hasHistoryData: true,
    historyDataFromDate: String(fromDate || "").trim() || null,
    historyDataToDate: String(toDate || "").trim() || null,
  };
};

const normalizeActiveStock = (row = {}) => ({
  ...row,
  ltp: toNumber(row.ltp),
  open: toNumber(row.open),
  high: toNumber(row.high),
  low: toNumber(row.low),
  close: toNumber(row.close),
  percentChange: toNumber(row.percent_change),
  avgPrice: toNumber(row.avg_price),
  lowerCircuit: toNumber(row.lower_circuit),
  upperCircuit: toNumber(row.upper_circuit),
  week52Low: toNumber(row.week52_low),
  week52High: toNumber(row.week52_high),
  security_code: row.security_code,
  master_is_active:
    row.master_is_active === undefined ? undefined : Boolean(row.master_is_active),
  ...parseHistoryRange(row.history_range),
});

const create = async (payload, db = pool) => {
  const params = [
    Number(payload.master_id),
    payload.token,
    payload.symbol,
    payload.name,
    payload.exchange,
    payload.instrumenttype || "EQ",
    String(payload.security_code ?? payload.securityCode ?? "").trim() || null,
    toNumber(payload.ltp, 0),
    toNumber(payload.open, 0),
    toNumber(payload.high, 0),
    toNumber(payload.low, 0),
    toNumber(payload.close, 0),
    toNumber(payload.percent_change ?? payload.percentChange, 0),
    toNumber(payload.avg_price ?? payload.avgPrice, 0),
    toNumber(payload.lower_circuit ?? payload.lowerCircuit, 0),
    toNumber(payload.upper_circuit ?? payload.upperCircuit, 0),
    toNumber(payload.week52_low ?? payload.week52Low, 0),
    toNumber(payload.week52_high ?? payload.week52High, 0),
  ];

  const insertActive = async () => {
    await syncSerialSequence(db, "active_stock", "id");
    const { rows } = await db.query(
      `
        INSERT INTO active_stock (
          master_id, token, symbol, name, exchange, instrumenttype,
          security_code, ltp, open, high, low, close, percent_change, avg_price,
          lower_circuit, upper_circuit, week52_low, week52_high
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        RETURNING *
      `,
      params,
    );
    return rows[0] ? normalizeActiveStock(rows[0]) : null;
  };

  return insertActive();
};

const getByToken = async (token, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE token = $1 LIMIT 1`,
    [token],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE master_id = $1 LIMIT 1`,
    [Number(masterId)],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getBySymbolAndExchange = async (symbol, exchange, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE symbol = $1 AND exchange = $2 LIMIT 1`,
    [String(symbol ?? "").trim(), String(exchange ?? "").trim().toUpperCase()],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getBySymbol = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE symbol = $1 LIMIT 1`,
    [String(symbol ?? "").trim()],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByNormalizedSymbol = async (symbol, db = pool) => {
  const key = normalizeSymbolForMatch(symbol);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT a.*
      FROM active_stock a
      INNER JOIN stock_master sm ON sm.id = a.master_id
      WHERE sm.is_active = TRUE
        AND regexp_replace(regexp_replace(upper(a.symbol), '${TRAILING_SYMBOL_SUFFIX_PATTERN}', '', 'gi'),
                           '[^A-Z0-9]+', '', 'g') = $1
      LIMIT 1
    `,
    [key],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getByName = async (name, db = pool) => {
  const key = normalizeNameForMatch(name);
  if (!key) return null;
  const { rows } = await db.query(
    `
      SELECT *
      FROM active_stock
      WHERE regexp_replace(lower(name), '[^a-z0-9]+', '', 'g')
        = $1
      LIMIT 1
    `,
    [key.toLowerCase()],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const getBySecurityCode = async (securityCode, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM active_stock WHERE security_code = $1 LIMIT 1`,
    [String(securityCode ?? "").trim()],
  );
  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const listActive = async ({ page, limit, search = "" } = {}, db = pool) => {
  const queryValues = [];
  const where = ["sm.is_active = TRUE"];

  if (search && String(search).trim()) {
    queryValues.push(`%${String(search).trim().toLowerCase()}%`);
    const idx = queryValues.length;
    where.push(
      `(lower(a.name) LIKE $${idx} OR lower(a.symbol) LIKE $${idx} OR lower(a.token) LIKE $${idx})`,
    );
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  if (!page || !limit) {
    const { rows } = await db.query(
      `
        SELECT
          a.*,
          sm.is_active AS master_is_active,
          sm.history_range
        FROM active_stock a
        LEFT JOIN stock_master sm ON sm.id = a.master_id
        ${whereClause}
        ORDER BY a.added_at DESC
      `,
      queryValues,
    );
    return rows.map(normalizeActiveStock);
  }

  const offset = (Number(page) - 1) * Number(limit);
  queryValues.push(Number(limit));
  const limitIdx = queryValues.length;
  queryValues.push(offset);
  const offsetIdx = queryValues.length;

  const { rows } = await db.query(
    `
      SELECT
        a.*,
        sm.is_active AS master_is_active,
        sm.history_range
      FROM active_stock a
      LEFT JOIN stock_master sm ON sm.id = a.master_id
      ${whereClause}
      ORDER BY a.added_at DESC
      LIMIT $${limitIdx} OFFSET $${offsetIdx}
    `,
    queryValues,
  );
  return rows.map(normalizeActiveStock);
};

const listByMasterIds = async (masterIds = [], db = pool) => {
  const ids = Array.from(new Set(masterIds.map(Number).filter((n) => Number.isFinite(n) && n > 0)));
  if (!ids.length) return [];
  const { rows } = await db.query(
    `
      SELECT a.*, sm.is_active AS master_is_active
      FROM active_stock a
      LEFT JOIN stock_master sm ON sm.id = a.master_id
      WHERE a.master_id = ANY($1::bigint[])
        AND sm.is_active = TRUE
    `,
    [ids],
  );
  return rows.map(normalizeActiveStock);
};

const updateByToken = async (token, data = {}, db = pool) => {
  const map = {
    percentChange: "percent_change",
    avgPrice: "avg_price",
    lowerCircuit: "lower_circuit",
    upperCircuit: "upper_circuit",
    week52Low: "week52_low",
    week52High: "week52_high",
    securityCode: "security_code",
  };

  const sets = [];
  const values = [token];
  Object.entries(data).forEach(([key, value]) => {
    if (key === "updatedAt" || key === "last_update" || key === "is_active") return;
    const col = map[key] || key;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });

  if (!sets.length) return getByToken(token, db);

  values.push(new Date());
  sets.push(`last_update = $${values.length}`);

  const { rows } = await db.query(
    `UPDATE active_stock SET ${sets.join(", ")} WHERE token = $1 RETURNING *`,
    values,
  );

  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const updateByMasterId = async (masterId, data = {}, db = pool) => {
  const map = {
    percentChange: "percent_change",
    avgPrice: "avg_price",
    lowerCircuit: "lower_circuit",
    upperCircuit: "upper_circuit",
    week52Low: "week52_low",
    week52High: "week52_high",
    securityCode: "security_code",
  };

  const sets = [];
  const values = [Number(masterId)];
  Object.entries(data).forEach(([key, value]) => {
    if (key === "updatedAt" || key === "last_update" || key === "is_active") return;
    const col = map[key] || key;
    values.push(value);
    sets.push(`${col} = $${values.length}`);
  });

  if (!sets.length) return getByMasterId(masterId, db);

  values.push(new Date());
  sets.push(`last_update = $${values.length}`);

  const { rows } = await db.query(
    `UPDATE active_stock SET ${sets.join(", ")} WHERE master_id = $1 RETURNING *`,
    values,
  );

  return rows[0] ? normalizeActiveStock(rows[0]) : null;
};

const toggleByToken = async (token, db = pool) => {
  const { rows: stockRows } = await db.query(
    `SELECT * FROM active_stock WHERE token = $1 LIMIT 1`,
    [token],
  );
  const stock = stockRows[0];
  if (!stock) return null;

  const { rows: masterRows } = await db.query(
    `
      UPDATE stock_master
      SET is_active = NOT is_active, updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [Number(stock.master_id)],
  );

  const master = masterRows[0];
  if (!master) return null;

  return normalizeActiveStock({
    ...stock,
    master_is_active: master.is_active,
  });
};

const deleteByToken = async (token, db = pool) => {
  const { rows: stockRows } = await db.query(
    `SELECT * FROM active_stock WHERE token = $1 LIMIT 1`,
    [token],
  );
  const stock = stockRows[0];
  if (!stock) return false;

  const { rowCount } = await db.query(
    `
      UPDATE stock_master
      SET is_active = FALSE, updated_at = NOW()
      WHERE id = $1
    `,
    [Number(stock.master_id)],
  );
  return rowCount > 0;
};

const bulkUpsertByToken = async (stocks = [], fields = [], db = pool) => {
  if (!stocks.length) return;

  for (const stock of stocks) {
    const updates = {};
    fields.forEach((f) => {
      if (stock[f] !== undefined) updates[f] = stock[f];
    });
    updates.last_update = new Date();

    const setCols = Object.keys(updates)
      .map((key, idx) => {
        const map = {
          percentChange: "percent_change",
          avgPrice: "avg_price",
          lowerCircuit: "lower_circuit",
          upperCircuit: "upper_circuit",
          week52Low: "week52_low",
          week52High: "week52_high",
        securityCode: "security_code",
        updatedAt: "last_update",
        last_update: "last_update",
        is_active: null,
      };
        if (map[key] === null) return null;
        return `${map[key] || key} = $${idx + 2}`;
      })
      .filter(Boolean)
      .join(", ");

    const values = [stock.token, ...Object.keys(updates).map((k) => updates[k])];
    await db.query(`UPDATE active_stock SET ${setCols} WHERE token = $1`, values);
  }
};

module.exports = {
  normalizeActiveStock,
  create,
  getByToken,
  getByMasterId,
  getBySymbolAndExchange,
  getBySymbol,
  getByNormalizedSymbol,
  getByName,
  getBySecurityCode,
  listActive,
  listByMasterIds,
  updateByToken,
  updateByMasterId,
  toggleByToken,
  deleteByToken,
  bulkUpsertByToken,
};
