const { pool } = require("../config/db");
const { toNumber } = require("./common");

const normalizeMaster = (row = {}) => ({
  ...row,
  fetch_count: toNumber(row.fetch_count),
  screener_status: row.screener_status || "PENDING",
});

const create = async (payload, db = pool) => {
  const { rows } = await db.query(
    `
      INSERT INTO stock_master (
        company, symbol, exchange, name, sector, industry, screener_url,
        screener_status, fetch_count, is_active, token, raw_stock_id
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      RETURNING *
    `,
    [
      payload.company ?? null,
      payload.symbol,
      payload.exchange,
      payload.name,
      payload.sector ?? null,
      payload.industry ?? null,
      payload.screener_url ?? "",
      payload.screener_status ?? "PENDING",
      toNumber(payload.fetch_count, 0),
      payload.is_active ?? true,
      payload.token,
      payload.raw_stock_id ?? payload.rawStockId ?? null,
    ],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const updateById = async (id, data = {}, db = pool) => {
  const sets = [];
  const values = [Number(id)];
  Object.entries(data).forEach(([key, value]) => {
    const col = key === "rawStockId" ? "raw_stock_id" : key;
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

const getBySymbolOrName = async (symbol, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE symbol = $1 OR name = $1 LIMIT 1`,
    [symbol],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const getByName = async (name, db = pool) => {
  const { rows } = await db.query(
    `SELECT * FROM stock_master WHERE name = $1 LIMIT 1`,
    [name],
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
    where.push(`(lower(name) LIKE $${values.length} OR lower(company) LIKE $${values.length})`);
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

const setFetchCount = async (id, count, db = pool) => {
  const { rows } = await db.query(
    `UPDATE stock_master SET fetch_count = $1, updated_at = NOW() WHERE id = $2 RETURNING *`,
    [Number(count), Number(id)],
  );
  return rows[0] ? normalizeMaster(rows[0]) : null;
};

const syncCompanyFromFundamentals = async (db = pool) => {
  const result = await db.query(
    `
      UPDATE stock_master AS sm
      SET company = sf.company,
          updated_at = NOW()
      FROM stock_screener_fundamentals AS sf
      WHERE sf.master_id = sm.id
        AND sf.company IS NOT NULL
        AND sf.company <> ''
        AND (sm.company IS NULL OR sm.company = '')
    `,
  );
  return { modified: result.rowCount || 0 };
};

const updateHistoryCoverage = async (
  id,
  {
    requestedFromDate = null,
    requestedToDate = null,
    actualFromDate = null,
    actualToDate = null,
  } = {},
  db = pool,
) => {
  const { rows } = await db.query(
    `
      UPDATE stock_master
      SET
        history_requested_from_date = COALESCE($2::date, history_requested_from_date),
        history_requested_to_date = COALESCE($3::date, history_requested_to_date),
        history_data_from_date = CASE
          WHEN $4::date IS NULL THEN history_data_from_date
          WHEN history_data_from_date IS NULL THEN $4::date
          ELSE LEAST(history_data_from_date, $4::date)
        END,
        history_data_to_date = CASE
          WHEN $5::date IS NULL THEN history_data_to_date
          WHEN history_data_to_date IS NULL THEN $5::date
          ELSE GREATEST(history_data_to_date, $5::date)
        END,
        has_history_data = CASE
          WHEN (history_data_from_date IS NOT NULL OR $4::date IS NOT NULL)
           AND (history_data_to_date IS NOT NULL OR $5::date IS NOT NULL)
            THEN TRUE
          ELSE has_history_data
        END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [Number(id), requestedFromDate, requestedToDate, actualFromDate, actualToDate],
  );

  return rows[0] ? normalizeMaster(rows[0]) : null;
};

module.exports = {
  normalizeMaster,
  create,
  updateById,
  getById,
  getByToken,
  getBySymbolOrName,
  getByName,
  listActive,
  list,
  setFetchCount,
  syncCompanyFromFundamentals,
  updateHistoryCoverage,
};
