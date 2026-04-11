const fs = require("fs");
const path = require("path");
const { getPool, ensureSchema } = require("../db/newsIngest.db");

const MUSIC_ROOT = path.join(__dirname, "..", "..", "music-library");
const MUSIC_UPLOAD_DIR = path.join(MUSIC_ROOT, "uploads");

function ensureMusicDirs() {
  fs.mkdirSync(MUSIC_UPLOAD_DIR, { recursive: true });
}

function sanitizeFileName(name) {
  return String(name || "")
    .replace(/[^\w.\-]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseDataUrl(dataUrl) {
  const raw = String(dataUrl || "");
  const match = raw.match(/^data:([^;]+);base64,(.+)$/);
  if (!match) throw new Error("music dataUrl must be a valid base64 audio data URL");
  return { mime: match[1], data: Buffer.from(match[2], "base64") };
}

function mimeToExtension(mime) {
  const lower = String(mime || "").toLowerCase();
  if (lower.includes("mpeg") || lower.includes("mp3")) return ".mp3";
  if (lower.includes("wav")) return ".wav";
  if (lower.includes("ogg")) return ".ogg";
  if (lower.includes("m4a") || lower.includes("mp4")) return ".m4a";
  if (lower.includes("aac")) return ".aac";
  return ".mp3";
}

async function listCategories({ userId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT id, user_id, category_name, created_at, updated_at
      FROM music_library_categories
      WHERE user_id = 0 OR user_id = $1
      ORDER BY category_name ASC
    `,
    [Number(userId || 0)],
  );
  return res.rows || [];
}

async function createCategory({ userId, categoryName }) {
  await ensureSchema();
  const db = getPool();
  const name = String(categoryName || "").trim();
  if (!name) throw new Error("categoryName is required");
  const res = await db.query(
    `
      INSERT INTO music_library_categories (user_id, category_name)
      VALUES ($1, $2)
      ON CONFLICT (user_id, category_name) DO UPDATE SET updated_at = NOW()
      RETURNING id, user_id, category_name, created_at, updated_at
    `,
    [Number(userId || 0), name],
  );
  return res.rows?.[0] || null;
}

async function listTracks({ userId, search = "", categoryId = null }) {
  await ensureSchema();
  const db = getPool();
  const values = [Number(userId || 0)];
  let where = `t.user_id = $1`;
  let idx = 2;
  if (search) {
    where += ` AND (t.title ILIKE $${idx} OR t.original_file_name ILIKE $${idx})`;
    values.push(`%${String(search).trim()}%`);
    idx += 1;
  }
  if (categoryId) {
    where += ` AND EXISTS (
      SELECT 1
      FROM music_library_track_categories tc
      WHERE tc.track_id = t.id AND tc.category_id = $${idx}
    )`;
    values.push(Number(categoryId));
    idx += 1;
  }

  const res = await db.query(
    `
      SELECT
        t.id,
        t.user_id,
        t.title,
        t.media_type,
        t.file_name,
        t.original_file_name,
        t.file_url,
        t.mime_type,
        t.duration_seconds,
        t.created_at,
        t.updated_at,
        COALESCE(
          JSON_AGG(
            DISTINCT JSONB_BUILD_OBJECT(
              'id', c.id,
              'categoryName', c.category_name
            )
          ) FILTER (WHERE c.id IS NOT NULL),
          '[]'::json
        ) AS categories
      FROM music_library_tracks t
      LEFT JOIN music_library_track_categories tc ON tc.track_id = t.id
      LEFT JOIN music_library_categories c ON c.id = tc.category_id
      WHERE ${where}
      GROUP BY t.id
      ORDER BY t.created_at DESC
    `,
    values,
  );
  return res.rows || [];
}

async function getTrackById({ userId, id }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT
        t.id,
        t.user_id,
        t.title,
        t.media_type,
        t.file_name,
        t.original_file_name,
        t.file_url,
        t.mime_type,
        t.duration_seconds,
        t.created_at,
        t.updated_at,
        COALESCE(
          JSON_AGG(
            DISTINCT JSONB_BUILD_OBJECT(
              'id', c.id,
              'categoryName', c.category_name
            )
          ) FILTER (WHERE c.id IS NOT NULL),
          '[]'::json
        ) AS categories
      FROM music_library_tracks t
      LEFT JOIN music_library_track_categories tc ON tc.track_id = t.id
      LEFT JOIN music_library_categories c ON c.id = tc.category_id
      WHERE t.user_id = $1 AND t.id = $2
      GROUP BY t.id
      LIMIT 1
    `,
    [Number(userId || 0), Number(id)],
  );
  return res.rows?.[0] || null;
}

async function uploadTrack({ userId, title, mediaType = "music", fileName, dataUrl, sourceUrl, categoryIds = [] }) {
  await ensureSchema();
  ensureMusicDirs();
  const db = getPool();
  let mime = "audio/mpeg";
  let data = null;
  if (dataUrl) {
    const parsed = parseDataUrl(dataUrl);
    mime = parsed.mime;
    data = parsed.data;
  } else if (sourceUrl) {
    const fetched = await fetch(sourceUrl);
    if (!fetched.ok) throw new Error(`Failed to fetch music asset (${fetched.status})`);
    mime = fetched.headers.get("content-type") || mime;
    data = Buffer.from(await fetched.arrayBuffer());
  } else {
    throw new Error("dataUrl or sourceUrl is required");
  }

  const ext = mimeToExtension(mime);
  const safeBase = sanitizeFileName(fileName || `music-${Date.now()}`);
  const baseName = safeBase.replace(/\.[a-zA-Z0-9]{1,10}$/, "") || `music-${Date.now()}`;
  const finalName = `${Date.now()}-${baseName}${ext}`;
  const outPath = path.join(MUSIC_UPLOAD_DIR, finalName);
  fs.writeFileSync(outPath, data);

  const publicUrl = `content/news-content/music-library/files/${finalName}`;
  const insertRes = await db.query(
    `
      INSERT INTO music_library_tracks (user_id, title, media_type, file_name, original_file_name, file_url, mime_type)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id, user_id, title, media_type, file_name, original_file_name, file_url, mime_type, duration_seconds, created_at, updated_at
    `,
    [
      Number(userId || 0),
      String(title || path.parse(fileName || finalName).name || "Music"),
      String(mediaType || "music").trim().toLowerCase() === "song" ? "song" : "music",
      finalName,
      String(fileName || finalName),
      publicUrl,
      mime,
    ],
  );
  const track = insertRes.rows?.[0] || null;
  if (!track) return null;

  const uniqueCategoryIds = Array.from(new Set((Array.isArray(categoryIds) ? categoryIds : []).map((id) => Number(id)).filter(Boolean)));
  for (const categoryId of uniqueCategoryIds) {
    await db.query(
      `
        INSERT INTO music_library_track_categories (track_id, category_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
      `,
      [track.id, categoryId],
    );
  }

  return getTrackById({ userId, id: track.id });
}

async function updateTrack({ userId, id, title, categoryIds = [] }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      UPDATE music_library_tracks
      SET title = COALESCE($3, title),
          updated_at = NOW()
      WHERE user_id = $1 AND id = $2
      RETURNING id, user_id, title, media_type, file_name, original_file_name, file_url, mime_type, duration_seconds, created_at, updated_at
    `,
    [Number(userId || 0), Number(id), title ? String(title) : null],
  );
  const track = res.rows?.[0] || null;
  if (!track) return null;

  await db.query(`DELETE FROM music_library_track_categories WHERE track_id = $1`, [Number(id)]);
  const uniqueCategoryIds = Array.from(new Set((Array.isArray(categoryIds) ? categoryIds : []).map((v) => Number(v)).filter(Boolean)));
  for (const categoryId of uniqueCategoryIds) {
    await db.query(
      `
        INSERT INTO music_library_track_categories (track_id, category_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
      `,
      [Number(id), categoryId],
    );
  }

  return getTrackById({ userId, id: track.id });
}

function getTrackFilePath(fileName) {
  const safe = path.basename(String(fileName || ""));
  if (!safe || safe !== String(fileName || "")) throw new Error("Invalid music file name");
  return path.join(MUSIC_UPLOAD_DIR, safe);
}

module.exports = {
  listCategories,
  createCategory,
  listTracks,
  getTrackById,
  uploadTrack,
  updateTrack,
  getTrackFilePath,
};
