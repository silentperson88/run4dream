const { pool } = require("../config/db");

const UPSERT_SQL = `
  INSERT INTO live_ipo_gmp (
    name,
    status,
    gain_price,
    gain_percentage,
    ai_score,
    rating,
    subscribed,
    price,
    ipo_size,
    lot,
    open_date,
    open_gmp,
    close_date,
    close_gmp,
    boarding_date,
    listing_date,
    listing_gmp,
    institutional_backing,
    type
  )
  VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
  )
  ON CONFLICT (name)
  DO UPDATE SET
    status = EXCLUDED.status,
    gain_price = EXCLUDED.gain_price,
    gain_percentage = EXCLUDED.gain_percentage,
    ai_score = EXCLUDED.ai_score,
    rating = EXCLUDED.rating,
    subscribed = EXCLUDED.subscribed,
    price = EXCLUDED.price,
    ipo_size = EXCLUDED.ipo_size,
    lot = EXCLUDED.lot,
    open_date = EXCLUDED.open_date,
    open_gmp = EXCLUDED.open_gmp,
    close_date = EXCLUDED.close_date,
    close_gmp = EXCLUDED.close_gmp,
    boarding_date = EXCLUDED.boarding_date,
    listing_date = EXCLUDED.listing_date,
    listing_gmp = EXCLUDED.listing_gmp,
    institutional_backing = EXCLUDED.institutional_backing,
    type = EXCLUDED.type
  RETURNING id, name, status, gain_price, gain_percentage, ai_score, rating, subscribed, price, ipo_size, lot, open_date, open_gmp, close_date, close_gmp, boarding_date, listing_date, listing_gmp, institutional_backing, type, (xmax = 0) AS inserted
`;

const upsertLiveIpoRows = async (rows = [], db = pool) => {
  let inserted = 0;
  let updated = 0;
  const items = [];

  for (const row of rows) {
    const { rows: savedRows } = await db.query(UPSERT_SQL, [
      row.name,
      row.status || null,
      row.gain_price || null,
      row.gain_percentage || null,
      row.ai_score ?? null,
      row.rating ?? null,
      row.subscribed || null,
      row.price || null,
      row.ipo_size || null,
      row.lot || null,
      row.open_date || null,
      row.open_gmp || null,
      row.close_date || null,
      row.close_gmp || null,
      row.boarding_date || null,
      row.listing_date || null,
      row.listing_gmp || null,
      row.institutional_backing ?? 0,
      row.type || null,
    ]);

    const saved = savedRows[0];
    if (!saved) continue;
    if (saved.inserted) inserted += 1;
    else updated += 1;
    items.push(saved);
  }

  return {
    total: inserted + updated,
    inserted,
    updated,
    items,
  };
};

const listLiveIpoRows = async ({ page = 1, limit = 50 } = {}, db = pool) => {
  const safePage = Number.isFinite(Number(page)) && Number(page) > 0 ? Number(page) : 1;
  const safeLimit =
    Number.isFinite(Number(limit)) && Number(limit) > 0
      ? Math.min(Number(limit), 200)
      : 50;
  const offset = (safePage - 1) * safeLimit;

  const [{ rows: countRows }, { rows: dataRows }] = await Promise.all([
    db.query("SELECT COUNT(*)::int AS total FROM live_ipo_gmp"),
    db.query(
      `
        SELECT
          id, name, status, gain_price, gain_percentage, ai_score, rating,
          subscribed, price, ipo_size, lot, open_date, open_gmp,
          close_date, close_gmp, boarding_date, listing_date, listing_gmp,
          institutional_backing, type
        FROM live_ipo_gmp
        ORDER BY id DESC
        LIMIT $1 OFFSET $2
      `,
      [safeLimit, offset],
    ),
  ]);

  const total = countRows[0]?.total || 0;
  const total_pages = total > 0 ? Math.ceil(total / safeLimit) : 0;

  return {
    items: dataRows,
    pagination: {
      page: safePage,
      limit: safeLimit,
      total,
      total_pages,
    },
  };
};

module.exports = {
  upsertLiveIpoRows,
  listLiveIpoRows,
};
