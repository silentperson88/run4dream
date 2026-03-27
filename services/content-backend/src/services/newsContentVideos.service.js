const fs = require("fs");
const path = require("path");
const { getPool, ensureSchema } = require("../db/newsIngest.db");

const ASSET_ROOT = path.join(__dirname, "..", "..", "KhabaroKiDuniya24-asset");
const ASSET_IMAGES = path.join(ASSET_ROOT, "images");
const ASSET_AUDIO = path.join(ASSET_ROOT, "audio");

function ensureAssetDirs() {
  fs.mkdirSync(ASSET_IMAGES, { recursive: true });
  fs.mkdirSync(ASSET_AUDIO, { recursive: true });
}

function sanitizeFileName(name) {
  return String(name || "")
    .replace(/[^\w.\-]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseDataUrl(dataUrl) {
  const raw = String(dataUrl || "");
  const match = raw.match(/^data:([a-zA-Z0-9.+-\/]+);base64,(.+)$/);
  if (!match) throw new Error("dataUrl must be a valid base64 image/audio data URL");
  return { mime: match[1], data: Buffer.from(match[2], "base64") };
}

async function fetchBinary(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to download asset");
  const arrayBuffer = await res.arrayBuffer();
  const mime = res.headers.get("content-type") || "application/octet-stream";
  return { mime, data: Buffer.from(arrayBuffer) };
}

async function listVideos({ userId, limit = 50, offset = 0 }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT id, user_id, language, script, clip_approach, sentence_keywords, audio_url, clips, status, render_job_id, file_name, video_url, created_at, updated_at
      FROM news_content_videos
      WHERE user_id = $1
      ORDER BY created_at DESC
      LIMIT $2 OFFSET $3
    `,
    [Number(userId), Number(limit), Number(offset)],
  );
  return res.rows || [];
}

async function createVideo({ userId, language = "english" }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      INSERT INTO news_content_videos (user_id, language)
      VALUES ($1, $2)
      RETURNING id, user_id, language, script, clip_approach, sentence_keywords, audio_url, clips, status, created_at, updated_at
    `,
    [Number(userId), String(language || "english")],
  );
  return res.rows?.[0];
}

async function getVideo({ userId, id }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT id, user_id, language, script, clip_approach, sentence_keywords, audio_url, clips, status, render_job_id, file_name, video_url, created_at, updated_at
      FROM news_content_videos
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [Number(userId), Number(id)],
  );
  return res.rows?.[0] || null;
}

async function updateVideo({ userId, id, language, script, clipApproach, sentenceKeywords, audioUrl, clips, status }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      UPDATE news_content_videos
      SET language = COALESCE($3, language),
          script = COALESCE($4, script),
          clip_approach = COALESCE($5, clip_approach),
          sentence_keywords = COALESCE($6, sentence_keywords),
          audio_url = COALESCE($7, audio_url),
          clips = COALESCE($8, clips),
          status = COALESCE($9, status),
          updated_at = NOW()
      WHERE user_id = $1 AND id = $2
      RETURNING id, user_id, language, script, clip_approach, sentence_keywords, audio_url, clips, status, created_at, updated_at
    `,
    [
      Number(userId),
      Number(id),
      language ? String(language) : null,
      typeof script === "string" ? script : null,
      clipApproach ? String(clipApproach) : null,
      sentenceKeywords ? JSON.stringify(sentenceKeywords) : null,
      typeof audioUrl === "string" ? audioUrl : null,
      clips ? JSON.stringify(clips) : null,
      status ? String(status) : null,
    ],
  );
  return res.rows?.[0] || null;
}

async function updateRenderInfo({ userId, id, renderJobId, fileName, videoUrl, status }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      UPDATE news_content_videos
      SET render_job_id = COALESCE($3, render_job_id),
          file_name = COALESCE($4, file_name),
          video_url = COALESCE($5, video_url),
          status = COALESCE($6, status),
          updated_at = NOW()
      WHERE user_id = $1 AND id = $2
      RETURNING id, user_id, language, script, audio_url, clips, status, render_job_id, file_name, video_url, created_at, updated_at
    `,
    [Number(userId), Number(id), renderJobId || null, fileName || null, videoUrl || null, status || null],
  );
  return res.rows?.[0] || null;
}

async function saveImageAsset({ userId, videoId, dataUrl, sourceUrl, fileName }) {
  if (!dataUrl && !sourceUrl) throw new Error("dataUrl or sourceUrl is required");
  ensureAssetDirs();
  const { mime, data } = dataUrl ? parseDataUrl(dataUrl) : await fetchBinary(sourceUrl);
  const ext = mime.includes("png") ? ".png" : mime.includes("jpeg") || mime.includes("jpg") ? ".jpg" : ".bin";
  const safeName = sanitizeFileName(fileName || `img-${Date.now()}-${Math.random().toString(16).slice(2)}${ext}`);
  const finalName = safeName.endsWith(ext) ? safeName : `${safeName}${ext}`;
  const targetPath = path.join(ASSET_IMAGES, finalName);
  fs.writeFileSync(targetPath, data);
  const url = `content/news-content/videos/assets/images/${finalName}`;
  return { fileName: finalName, url };
}

async function saveAudioAsset({ userId, videoId, dataUrl, fileName }) {
  if (!dataUrl) throw new Error("dataUrl is required");
  ensureAssetDirs();
  const { mime, data } = parseDataUrl(dataUrl);
  const ext = mime.includes("wav") ? ".wav" : mime.includes("mpeg") ? ".mp3" : ".bin";
  const safeName = sanitizeFileName(fileName || `audio-${Date.now()}-${Math.random().toString(16).slice(2)}${ext}`);
  const finalName = safeName.endsWith(ext) ? safeName : `${safeName}${ext}`;
  const targetPath = path.join(ASSET_AUDIO, finalName);
  fs.writeFileSync(targetPath, data);
  const url = `content/news-content/videos/assets/audio/${finalName}`;
  return { fileName: finalName, url };
}

function getImagePath(fileName) {
  return path.join(ASSET_IMAGES, fileName);
}

function getAudioPath(fileName) {
  return path.join(ASSET_AUDIO, fileName);
}

module.exports = {
  listVideos,
  createVideo,
  getVideo,
  updateVideo,
  updateRenderInfo,
  saveImageAsset,
  saveAudioAsset,
  getImagePath,
  getAudioPath,
};
