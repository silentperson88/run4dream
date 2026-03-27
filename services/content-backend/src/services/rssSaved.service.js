const { getPool, ensureSchema } = require("../db/newsIngest.db");

async function saveRssItem({ userId, source, title, link, pubDate }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      INSERT INTO news_rss_items (user_id, source, title, link, pub_date)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (link) DO NOTHING
      RETURNING id, user_id, source, title, link, pub_date, created_at
    `,
    [
      Number(userId),
      String(source || "unknown"),
      String(title || ""),
      String(link || ""),
      pubDate ? String(pubDate) : null,
    ],
  );
  return res.rows?.[0] || null;
}

module.exports = {
  saveRssItem,
  listRssItems,
  getRssProgress,
  lockNextPending,
  markInProgress,
  markCompleted,
  markFailed,
  getRssByLink,
  linkVideoById,
  markProcessedById,
};

async function listRssItems({ userId, limit = 100, offset = 0, fromDate, toDate }) {
  await ensureSchema();
  const db = getPool();
  const filters = [`user_id = $1`];
  const values = [Number(userId)];
  let paramIdx = values.length + 1;

  if (fromDate) {
    filters.push(`DATE(created_at) >= $${paramIdx}`);
    values.push(String(fromDate));
    paramIdx += 1;
  }

  if (toDate) {
    filters.push(`DATE(created_at) <= $${paramIdx}`);
    values.push(String(toDate));
    paramIdx += 1;
  }

  values.push(Number(limit), Number(offset));
  const limitIdx = values.length - 1;
  const offsetIdx = values.length;
  const res = await db.query(
    `
      SELECT id, user_id, source, title, link, pub_date, status, raw_text, cleaned_text, images, template_one, template_two, template_three, template_generated_at, news_content_video_id, error, created_at, finished_at
      FROM news_rss_items
      WHERE ${filters.join(" AND ")}
      ORDER BY created_at DESC
      LIMIT $${limitIdx} OFFSET $${offsetIdx}
    `,
    values
  );
  return res.rows || [];
}

async function getRssProgress({ userId, fromDate, toDate }) {
  await ensureSchema();
  const db = getPool();
  const filters = [`user_id = $1`];
  const values = [Number(userId)];
  let paramIdx = values.length + 1;

  if (fromDate) {
    filters.push(`DATE(created_at) >= $${paramIdx}`);
    values.push(String(fromDate));
    paramIdx += 1;
  }

  if (toDate) {
    filters.push(`DATE(created_at) <= $${paramIdx}`);
    values.push(String(toDate));
    paramIdx += 1;
  }
  const res = await db.query(
    `
      SELECT status, COUNT(*)::int AS count
      FROM news_rss_items
      WHERE ${filters.join(" AND ")}
      GROUP BY status
    `,
    values
  );
  const counts = { pending: 0, processing: 0, completed: 0, failed: 0 };
  (res.rows || []).forEach(row => {
    const key = String(row.status || "").toLowerCase();
    if (counts[key] !== undefined) counts[key] = Number(row.count || 0);
  });
  return counts;
}

async function lockNextPending() {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      UPDATE news_rss_items
      SET status = 'processing',
          started_at = NOW(),
          attempts = attempts + 1
      WHERE id = (
        SELECT id FROM news_rss_items
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `
  );
  return res.rows?.[0] || null;
}

async function markInProgress({ id, rawText }) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      UPDATE news_rss_items
      SET raw_text = COALESCE($2, raw_text)
      WHERE id = $1
    `,
    [Number(id), rawText || null]
  );
}

async function markCompleted({ id, cleanedText }) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      UPDATE news_rss_items
      SET status = 'completed',
          cleaned_text = $2,
          error = NULL,
          finished_at = NOW()
      WHERE id = $1
    `,
    [Number(id), cleanedText || null]
  );
}

async function markFailed({ id, error }) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      UPDATE news_rss_items
      SET status = 'failed',
          error = $2,
          finished_at = NOW()
      WHERE id = $1
    `,
    [Number(id), String(error || "Failed")]
  );
}

async function getRssByLink({ link }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT *
      FROM news_rss_items
      WHERE link = $1
      LIMIT 1
    `,
    [String(link)]
  );
  return res.rows?.[0] || null;
}

async function markProcessedById({ id, status, rawText, cleanedText, images, templateOne, templateTwo, templateThree, error }) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      UPDATE news_rss_items
      SET status = $2,
          raw_text = $3,
          cleaned_text = $4,
          images = $5::jsonb,
          template_one = $6::jsonb,
          template_two = $7::jsonb,
          template_three = $8::jsonb,
          template_generated_at = CASE WHEN $6::jsonb IS NOT NULL OR $7::jsonb IS NOT NULL OR $8::jsonb IS NOT NULL THEN NOW() ELSE template_generated_at END,
          error = $9,
          finished_at = NOW()
      WHERE id = $1
    `,
    [
      Number(id),
      String(status || "completed"),
      rawText ? String(rawText) : null,
      cleanedText ? String(cleanedText) : null,
      images ? JSON.stringify(images) : null,
      templateOne ? JSON.stringify(templateOne) : null,
      templateTwo ? JSON.stringify(templateTwo) : null,
      templateThree ? JSON.stringify(templateThree) : null,
      error ? String(error) : null,
    ]
  );
}

async function linkVideoById({ id, videoId }) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      UPDATE news_rss_items
      SET news_content_video_id = $2
      WHERE id = $1
    `,
    [Number(id), Number(videoId)]
  );
}
