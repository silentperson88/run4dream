const { URL } = require("url");
const pdfParse = require("pdf-parse");
const { ensureSchema, getPool } = require("../db/newsIngest.db");

const API_ENDPOINTS = [
  "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w",
  "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w",
];
const ATTACHMENT_BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/";
const DEFAULT_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  Accept: "application/json, text/plain, */*",
  Referer: "https://www.bseindia.com/corporates/ann.html",
  Origin: "https://www.bseindia.com",
};

const DEFAULT_THEME_TERMS = {
  preferential_shares: [
    "preferential shares",
    "preferential issue",
    "preferential allotment",
    "preferential basis",
    "preference shares",
    "private placement",
    "convertible preference shares",
    "non convertible preference shares",
  ],
  qip: [
    "qualified institutional placement",
    "qip",
    "qualified institutional buyers",
    "institutional placement programme",
    "institutional placement program",
    "placement document",
  ],
  capacity_expansion: [
    "capacity expansion",
    "capacity enhancement",
    "capacity increase",
    "capacity augmentation",
    "expansion of capacity",
    "plant expansion",
    "greenfield",
    "brownfield",
    "commercial production",
    "commissioning",
    "debottlenecking",
    "manufacturing capacity",
  ],
  ad_campaign_collaboration: [
    "ad campaign",
    "advertising campaign",
    "marketing campaign",
    "brand campaign",
    "campaign launch",
    "brand collaboration",
    "collaboration agreement",
    "strategic partnership",
    "co branding",
    "brand ambassador",
    "endorsement",
    "media campaign",
  ],
  presentation: [
    "presentation",
    "investor presentation",
    "corporate presentation",
    "analyst presentation",
    "earnings presentation",
  ],
  quarterly_results: [
    "quarterly results",
    "financial results",
    "unaudited financial results",
    "audited financial results",
    "results for the quarter",
    "earnings results",
    "q1 results",
    "q2 results",
    "q3 results",
    "q4 results",
    "re:\\bq[1-4]\\s*fy\\s*\\d{2,4}\\b",
  ],
};

const TEXT_FIELDS = [
  "HEADLINE",
  "NEWSSUB",
  "CATEGORYNAME",
  "ANNOUNCEMENT_TYPE",
  "SLONGNAME",
  "XML_NAME",
];

function cleanText(value) {
  return String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function todayDateStr() {
  const d = new Date();
  const yyyy = String(d.getFullYear());
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeCategory(value) {
  const raw = cleanText(value);
  return raw || "";
}

function normalizeMatchStatus(value) {
  const raw = String(value || "matched").trim().toLowerCase();
  if (["matched", "unmatched", "pending", "all"].includes(raw)) return raw;
  return "matched";
}

function parseDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return new Date(`${todayDateStr()}T00:00:00`);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return new Date(`${raw}T00:00:00`);
  if (/^\d{8}$/.test(raw)) {
    return new Date(`${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T00:00:00`);
  }
  throw new Error("date must be YYYY-MM-DD or YYYYMMDD");
}

function toBseDate(dateObj) {
  const yyyy = String(dateObj.getFullYear());
  const mm = String(dateObj.getMonth() + 1).padStart(2, "0");
  const dd = String(dateObj.getDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function parseExtraKeywords(raw) {
  return String(raw || "")
    .split(/[,;\n]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function compileFlexiblePhrase(phrase) {
  const tokens = String(phrase || "").match(/[A-Za-z0-9]+/g) || [];
  if (!tokens.length) return new RegExp(phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
  const pattern = `\\b${tokens.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("[\\s\\-/]*")}\\b`;
  return new RegExp(pattern, "i");
}

function buildPattern(term) {
  const raw = String(term || "").trim();
  if (raw.startsWith("re:")) {
    return { label: raw.slice(3), regex: new RegExp(raw.slice(3), "i") };
  }
  return { label: raw, regex: compileFlexiblePhrase(raw) };
}

class SemanticMatcher {
  constructor(extraKeywords = []) {
    this.patterns = {};
    Object.entries(DEFAULT_THEME_TERMS).forEach(([theme, terms]) => {
      this.patterns[theme] = terms.map((t) => buildPattern(t));
    });
    if (extraKeywords.length) {
      this.patterns.custom = extraKeywords.map((t) => buildPattern(t));
    }
  }

  matchText(text) {
    const src = cleanText(text);
    if (!src) return { score: 0, hits: {} };
    const hits = {};
    Object.entries(this.patterns).forEach(([theme, patterns]) => {
      const found = patterns.filter((p) => p.regex.test(src)).map((p) => p.label);
      if (found.length) hits[theme] = Array.from(new Set(found)).sort();
    });
    const score = Object.values(hits).reduce((sum, terms) => sum + terms.length, 0);
    return { score, hits };
  }
}

async function fetchJson(endpoint, payload, timeoutMs) {
  const u = new URL(endpoint);
  Object.entries(payload).forEach(([k, v]) => u.searchParams.set(k, String(v)));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(u.toString(), { headers: DEFAULT_HEADERS, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data || typeof data !== "object") throw new Error("Unexpected response shape");
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function findInt(obj, keys) {
  const lower = Object.fromEntries(Object.entries(obj || {}).map(([k, v]) => [String(k).toLowerCase(), v]));
  for (const k of keys) {
    const v = lower[k];
    if (v === undefined || v === null || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return Math.trunc(n);
  }
  return null;
}

function nextPage(responseData, currentPage) {
  const nextKeys = ["nextpage", "next_page", "nextpageno", "next_page_no"];
  const totalKeys = ["totalpages", "totalpage", "total_page", "totalpagecnt", "total_page_cnt"];
  const explicitNext = findInt(responseData, nextKeys);
  if (explicitNext !== null) return explicitNext > currentPage ? explicitNext : null;
  const table = Array.isArray(responseData?.Table) ? responseData.Table : [];
  if (table.length && typeof table[0] === "object") {
    const fromRow = findInt(table[0], totalKeys);
    if (fromRow !== null) return currentPage >= fromRow ? null : currentPage + 1;
  }
  const totalPages = findInt(responseData, totalKeys);
  if (totalPages !== null && currentPage >= totalPages) return null;
  return currentPage + 1;
}

async function iterAnnouncements({ basePayload, maxPages, timeoutMs }) {
  let page = 1;
  let activeEndpoint = "";
  const seen = new Set();
  const rows = [];

  while (true) {
    const payload = { ...basePayload, pageno: String(page), Pageno: String(page) };
    const endpoints = activeEndpoint
      ? [activeEndpoint, ...API_ENDPOINTS.filter((x) => x !== activeEndpoint)]
      : [...API_ENDPOINTS];
    let data = null;
    let lastErr = null;
    for (const endpoint of endpoints) {
      try {
        data = await fetchJson(endpoint, payload, timeoutMs);
        activeEndpoint = endpoint;
        break;
      } catch (err) {
        lastErr = err;
      }
    }
    if (!data) throw new Error(`Failed to fetch page ${page}: ${lastErr?.message || lastErr}`);

    const table = Array.isArray(data.Table) ? data.Table : [];
    if (!table.length) break;

    let newRows = 0;
    for (const row of table) {
      const rowId = cleanText(row?.NEWSID || row?.ATTACHMENTNAME || `${page}-${rows.length}`);
      if (seen.has(rowId)) continue;
      seen.add(rowId);
      rows.push(row);
      newRows += 1;
    }

    if (maxPages > 0 && page >= maxPages) break;
    if (newRows === 0) break;
    const np = nextPage(data, page);
    if (!np) break;
    page = np;
  }
  return rows;
}

function isPdfUrl(url) {
  try {
    const u = new URL(url);
    return u.pathname.toLowerCase().endsWith(".pdf");
  } catch (_) {
    return false;
  }
}

function extractPdfUrl(row) {
  const attachment = cleanText(row?.ATTACHMENTNAME);
  if (attachment) {
    const lower = attachment.toLowerCase();
    if ((lower.startsWith("http://") || lower.startsWith("https://")) && isPdfUrl(attachment)) return attachment;
    if (lower.endsWith(".pdf")) return ATTACHMENT_BASE_URL + attachment;
  }
  for (const key of ["NSURL", "MORE", "HEADLINE"]) {
    const value = cleanText(row?.[key]);
    if (!value) continue;
    const m = value.match(/https?:\/\/[^\s"'<>]+/i);
    if (m && isPdfUrl(m[0])) return m[0];
  }
  return "";
}

function composeAnnouncementText(row) {
  return TEXT_FIELDS.map((f) => cleanText(row?.[f])).filter(Boolean).join(" ").trim();
}

function mergeHits(metadataHits, pdfHits) {
  const merged = {};
  [metadataHits || {}, pdfHits || {}].forEach((src) => {
    Object.entries(src).forEach(([theme, terms]) => {
      if (!merged[theme]) merged[theme] = new Set();
      terms.forEach((t) => merged[theme].add(t));
    });
  });
  return Object.fromEntries(Object.entries(merged).map(([k, set]) => [k, Array.from(set).sort()]));
}

async function fetchBinary(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { headers: DEFAULT_HEADERS, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const ab = await res.arrayBuffer();
    return Buffer.from(ab);
  } finally {
    clearTimeout(timer);
  }
}

function looksLikePdf(content) {
  if (!content || !content.length) return false;
  const text = Buffer.from(content).slice(0, 12).toString("utf8");
  return text.includes("%PDF");
}

async function extractPdfText(content) {
  const parsed = await pdfParse(content);
  return cleanText(parsed?.text || "");
}

function mapItemRow(dbRow) {
  return {
    id: Number(dbRow.id),
    newsId: dbRow.news_id,
    newsDate: dbRow.news_date || "",
    disseminatedAt: dbRow.disseminated_at || "",
    scripCode: dbRow.scrip_code || "",
    company: dbRow.company || "",
    headline: dbRow.headline || "",
    category: dbRow.category || "",
    announcementType: dbRow.announcement_type || "",
    pdfUrl: dbRow.pdf_url || "",
    hasPdf: Boolean(dbRow.pdf_url),
    matchStatus: dbRow.match_status,
    matchSource: dbRow.match_source || "",
    matchScore: Number(dbRow.match_score || 0),
    metadataScore: Number(dbRow.metadata_score || 0),
    pdfScore: Number(dbRow.pdf_score || 0),
    matchedThemes: Array.isArray(dbRow.matched_themes) ? dbRow.matched_themes : [],
    matchedTerms: dbRow.matched_terms || {},
    pdfText: dbRow.pdf_text || "",
    importantPointsText: dbRow.important_points_text || "",
    scriptEnglish: dbRow.script_english || "",
    scriptHindi: dbRow.script_hindi || "",
    scriptAudioEnglish: dbRow.script_audio_english || "",
    scriptAudioHindi: dbRow.script_audio_hindi || "",
    highlightTerms: Array.isArray(dbRow.highlight_terms) ? dbRow.highlight_terms : [],
    highlightTermsPositive: Array.isArray(dbRow.highlight_terms_positive) ? dbRow.highlight_terms_positive : [],
    highlightTermsNegative: Array.isArray(dbRow.highlight_terms_negative) ? dbRow.highlight_terms_negative : [],
  };
}

async function getNewsList({
  userId,
  limit = 50,
  offset = 0,
  date,
  category,
  matchStatus,
  excludeUsedForVideo = false,
}) {
  await ensureSchema();
  const db = getPool();
  const safeLimit = Math.min(200, Math.max(1, Number(limit || 50)));
  const safeOffset = Math.max(0, Number(offset || 0));
  const filterDate = String(date || todayDateStr()).trim();
  const filterCategory = normalizeCategory(category);
  const filterStatus = normalizeMatchStatus(matchStatus);
  const excludeUsed = String(excludeUsedForVideo || "").toLowerCase() === "true" || excludeUsedForVideo === true;
  const where = ["user_id = $1", "fetched_for_date = $2::date"];
  const params = [userId, filterDate];
  let paramIndex = params.length + 1;

  if (filterCategory && filterCategory.toLowerCase() !== "all") {
    where.push(`category = $${paramIndex}`);
    params.push(filterCategory);
    paramIndex += 1;
  }
  if (filterStatus !== "all") {
    where.push(`match_status = $${paramIndex}`);
    params.push(filterStatus);
    paramIndex += 1;
  }
  if (excludeUsed && filterCategory && filterCategory.toLowerCase() !== "all") {
    where.push(`
      id NOT IN (
        SELECT DISTINCT n.news_row_id
        FROM bse_full_video_news n
        JOIN bse_full_videos v ON v.id = n.video_id
        WHERE v.user_id = $1
          AND v.video_date = $2::date
          AND v.status = 'completed'
      )
    `);
  }
  const whereSql = where.join(" AND ");

  const countRes = await db.query(
    `SELECT COUNT(*)::int AS total FROM bse_news WHERE ${whereSql}`,
    params,
  );
  const rowsRes = await db.query(
    `
      SELECT
        id, news_id, news_date, disseminated_at, scrip_code, company, headline, category,
        announcement_type, pdf_url, match_status, match_source, match_score, metadata_score, pdf_score, matched_themes, matched_terms, pdf_text,
        important_points_text, script_english, script_hindi, script_audio_english, script_audio_hindi, highlight_terms, highlight_terms_positive, highlight_terms_negative
      FROM bse_news
      WHERE ${whereSql}
      ORDER BY created_at DESC, id DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
    `,
    [...params, safeLimit, safeOffset],
  );

  const matchedRes = await db.query(
    `
      SELECT COUNT(*)::int AS matched
      FROM bse_news
      WHERE user_id = $1
        AND fetched_for_date = $2::date
        AND ($3::text = 'all' OR category = $3::text)
        AND match_status = 'matched'
    `,
    [userId, filterDate, filterCategory && filterCategory.toLowerCase() !== "all" ? filterCategory : "all"],
  );

  return {
    totalRows: Number(countRes.rows[0]?.total || 0),
    matchedRows: Number(matchedRes.rows[0]?.matched || 0),
    unmatchedRows: Number(countRes.rows[0]?.total || 0) - Number(matchedRes.rows[0]?.matched || 0),
    limit: safeLimit,
    offset: safeOffset,
    date: filterDate,
    category: filterCategory || "all",
    matchStatus: filterStatus,
    excludeUsedForVideo: Boolean(excludeUsed && filterCategory && filterCategory.toLowerCase() !== "all"),
    rows: rowsRes.rows.map(mapItemRow),
  };
}

async function getNewsCategories({ userId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT category_name
      FROM (
        SELECT category_name
        FROM bse_news_categories
        WHERE user_id = $1
        UNION
        SELECT DISTINCT category AS category_name
        FROM bse_news
        WHERE user_id = $1
          AND COALESCE(category, '') <> ''
      ) t
      ORDER BY category_name ASC
    `,
    [userId],
  );
  return res.rows.map((row) => String(row.category_name || "").trim()).filter(Boolean);
}

async function getNewsItem({ userId, id }) {
  await ensureSchema();
  const db = getPool();
  const newsId = Number(id);
  if (!Number.isFinite(newsId) || newsId <= 0) {
    throw new Error("Invalid news id");
  }
  const res = await db.query(
    `
      SELECT
        id, news_id, news_date, disseminated_at, scrip_code, company, headline, category,
        announcement_type, pdf_url, match_status, match_source, match_score, metadata_score, pdf_score,
        matched_themes, matched_terms, pdf_text, raw_data, important_points_text, script_english, script_hindi, script_audio_english, script_audio_hindi, highlight_terms, highlight_terms_positive, highlight_terms_negative
      FROM bse_news
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [userId, newsId],
  );
  if (!res.rows[0]) {
    throw new Error("News not found");
  }
  return mapItemRow(res.rows[0]);
}

async function getMatchedNewsForBatch({ userId, date, category = "all" }) {
  await ensureSchema();
  const db = getPool();
  const filterDate = String(date || todayDateStr()).trim();
  const filterCategory = normalizeCategory(category);
  const params = [userId, filterDate];
  const where = ["user_id = $1", "fetched_for_date = $2::date", "match_status = 'matched'"];

  if (filterCategory && filterCategory.toLowerCase() !== "all") {
    where.push(`category = $${params.length + 1}`);
    params.push(filterCategory);
  }

  const res = await db.query(
    `
      SELECT
        id, headline, company, category, news_date, fetched_for_date,
        important_points_text, script_english, script_audio_english, highlight_terms, highlight_terms_positive, highlight_terms_negative
      FROM bse_news
      WHERE ${where.join(" AND ")}
      ORDER BY created_at DESC, id DESC
    `,
    params,
  );

  return res.rows.map((row) => ({
    id: Number(row.id),
    headline: cleanText(row.headline),
    company: cleanText(row.company),
    category: cleanText(row.category),
    newsDate: cleanText(row.news_date),
    fetchedForDate: cleanText(row.fetched_for_date),
    importantPointsText: String(row.important_points_text || "").trim(),
    scriptEnglish: String(row.script_english || "").trim(),
    scriptAudioEnglish: String(row.script_audio_english || "").trim(),
    highlightTerms: Array.isArray(row.highlight_terms) ? row.highlight_terms : [],
    highlightTermsPositive: Array.isArray(row.highlight_terms_positive) ? row.highlight_terms_positive : [],
    highlightTermsNegative: Array.isArray(row.highlight_terms_negative) ? row.highlight_terms_negative : [],
  }));
}

async function updateNewsApproachData({
  userId,
  id,
  importantPointsText,
  scriptEnglish,
  scriptHindi,
  scriptAudioEnglish,
  scriptAudioHindi,
  highlightTerms,
  highlightTermsPositive,
  highlightTermsNegative,
}) {
  await ensureSchema();
  const db = getPool();
  const newsId = Number(id);
  if (!Number.isFinite(newsId) || newsId <= 0) {
    throw new Error("Invalid news id");
  }

  const fields = [];
  const params = [];
  let p = 1;
  const add = (column, value) => {
    if (value === undefined) return;
    fields.push(`${column} = $${p}`);
    params.push(value);
    p += 1;
  };

  add("important_points_text", typeof importantPointsText === "string" ? importantPointsText : undefined);
  add("script_english", typeof scriptEnglish === "string" ? scriptEnglish : undefined);
  add("script_hindi", typeof scriptHindi === "string" ? scriptHindi : undefined);
  add("script_audio_english", typeof scriptAudioEnglish === "string" ? scriptAudioEnglish : undefined);
  add("script_audio_hindi", typeof scriptAudioHindi === "string" ? scriptAudioHindi : undefined);
  add(
    "highlight_terms",
    Array.isArray(highlightTerms)
      ? highlightTerms.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 50)
      : undefined,
  );
  add(
    "highlight_terms_positive",
    Array.isArray(highlightTermsPositive)
      ? highlightTermsPositive.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 50)
      : undefined,
  );
  add(
    "highlight_terms_negative",
    Array.isArray(highlightTermsNegative)
      ? highlightTermsNegative.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 50)
      : undefined,
  );

  if (!fields.length) return null;

  params.push(userId, newsId);
  const userParam = p;
  const idParam = p + 1;

  const res = await db.query(
    `
      UPDATE bse_news
      SET ${fields.join(", ")}, updated_at = NOW()
      WHERE user_id = $${userParam} AND id = $${idParam}
      RETURNING
        id, news_id, news_date, disseminated_at, scrip_code, company, headline, category,
        announcement_type, pdf_url, match_status, match_source, match_score, metadata_score, pdf_score,
        matched_themes, matched_terms, pdf_text, raw_data, important_points_text, script_english, script_hindi, script_audio_english, script_audio_hindi, highlight_terms, highlight_terms_positive, highlight_terms_negative
    `,
    params,
  );

  if (!res.rows[0]) {
    throw new Error("News not found");
  }
  return mapItemRow(res.rows[0]);
}

async function resetNewsApproachData({ userId, id }) {
  await ensureSchema();
  const db = getPool();
  const newsId = Number(id);
  if (!Number.isFinite(newsId) || newsId <= 0) {
    throw new Error("Invalid news id");
  }
  await db.query(
    `
      UPDATE bse_news
      SET
        important_points_text = NULL,
        script_english = NULL,
        script_audio_english = NULL,
        highlight_terms = '{}',
        highlight_terms_positive = '{}',
        highlight_terms_negative = '{}',
        updated_at = NOW()
      WHERE user_id = $1 AND id = $2
    `,
    [userId, newsId],
  );
}

function mapJobRow(row) {
  if (!row) return null;
  return {
    id: String(row.id || ""),
    userId: Number(row.user_id || 0),
    status: String(row.status || "running"),
    date: String(row.date || ""),
    category: String(row.category || "all"),
    forcedMatchStatus: String(row.forced_match_status || "matched"),
    model: String(row.model || ""),
    total: Number(row.total || 0),
    processed: Number(row.processed || 0),
    success: Number(row.success || 0),
    failed: Number(row.failed || 0),
    skipped: Number(row.skipped || 0),
    currentNewsId: row.current_news_id === null || row.current_news_id === undefined ? null : Number(row.current_news_id),
    currentHeadline: String(row.current_headline || ""),
    gapMs: Number(row.gap_ms || 0),
    cancelRequested: Boolean(row.cancel_requested),
    errors: Array.isArray(row.errors) ? row.errors : [],
    startedAt: row.started_at || null,
    finishedAt: row.finished_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
  };
}

async function createNewsApproachJob(job) {
  await ensureSchema();
  const db = getPool();
  await db.query(
    `
      INSERT INTO news_approach_jobs (
        id, user_id, status, date, category, forced_match_status, model,
        total, processed, success, failed, skipped, current_news_id, current_headline,
        gap_ms, cancel_requested, errors, started_at, finished_at, updated_at
      ) VALUES (
        $1, $2, $3, $4::date, $5, $6, $7,
        $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17::jsonb, $18, $19, NOW()
      )
    `,
    [
      String(job.id || ""),
      Number(job.userId || 0),
      String(job.status || "running"),
      String(job.date || todayDateStr()),
      String(job.category || "all"),
      String(job.forcedMatchStatus || "matched"),
      String(job.model || ""),
      Number(job.total || 0),
      Number(job.processed || 0),
      Number(job.success || 0),
      Number(job.failed || 0),
      Number(job.skipped || 0),
      job.currentNewsId === null || job.currentNewsId === undefined ? null : Number(job.currentNewsId),
      String(job.currentHeadline || ""),
      Number(job.gapMs || 0),
      Boolean(job.cancelRequested),
      JSON.stringify(Array.isArray(job.errors) ? job.errors : []),
      job.startedAt || new Date().toISOString(),
      job.finishedAt || null,
    ],
  );
}

async function updateNewsApproachJob({ userId, jobId, patch }) {
  await ensureSchema();
  const db = getPool();
  const fields = [];
  const params = [];
  let p = 1;
  const add = (column, value, transform) => {
    if (value === undefined) return;
    fields.push(`${column} = $${p}`);
    params.push(transform ? transform(value) : value);
    p += 1;
  };

  add("status", patch.status, (v) => String(v));
  add("category", patch.category, (v) => String(v || "all"));
  add("model", patch.model, (v) => String(v || ""));
  add("total", patch.total, (v) => Number(v || 0));
  add("processed", patch.processed, (v) => Number(v || 0));
  add("success", patch.success, (v) => Number(v || 0));
  add("failed", patch.failed, (v) => Number(v || 0));
  add("skipped", patch.skipped, (v) => Number(v || 0));
  add("current_news_id", patch.currentNewsId, (v) => (v === null ? null : Number(v)));
  add("current_headline", patch.currentHeadline, (v) => String(v || ""));
  add("gap_ms", patch.gapMs, (v) => Number(v || 0));
  add("cancel_requested", patch.cancelRequested, (v) => Boolean(v));
  add("errors", patch.errors, (v) => JSON.stringify(Array.isArray(v) ? v : []));
  add("finished_at", patch.finishedAt, (v) => v || null);
  add("started_at", patch.startedAt, (v) => v || new Date().toISOString());

  if (!fields.length) return null;
  fields.push(`updated_at = NOW()`);
  params.push(Number(userId || 0), String(jobId || ""));

  const res = await db.query(
    `
      UPDATE news_approach_jobs
      SET ${fields.join(", ")}
      WHERE user_id = $${p} AND id = $${p + 1}
      RETURNING *
    `,
    params,
  );
  return mapJobRow(res.rows[0]);
}

async function getNewsApproachJob({ userId, jobId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT *
      FROM news_approach_jobs
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [Number(userId || 0), String(jobId || "")],
  );
  return mapJobRow(res.rows[0]);
}

async function requestStopNewsApproachJob({ userId, jobId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      UPDATE news_approach_jobs
      SET
        cancel_requested = TRUE,
        status = CASE WHEN status = 'running' THEN 'stopping' ELSE status END,
        updated_at = NOW()
      WHERE user_id = $1 AND id = $2
      RETURNING *
    `,
    [Number(userId || 0), String(jobId || "")],
  );
  return mapJobRow(res.rows[0]);
}

async function fetchAndStoreNews({
  userId,
  date,
  category,
  scrip,
  search,
  annType,
  subcategory,
  minScore,
  maxPages,
  timeoutMs,
  extraKeywords,
  limit,
  offset,
}) {
  await ensureSchema();
  const db = getPool();
  const parsedDate = parseDate(date || "");
  const requestDate = `${parsedDate.getFullYear()}-${String(parsedDate.getMonth() + 1).padStart(2, "0")}-${String(parsedDate.getDate()).padStart(2, "0")}`;
  const bseDate = toBseDate(parsedDate);
  const matchThreshold = Number.isFinite(Number(minScore)) ? Number(minScore) : 0;
  const matcher = new SemanticMatcher(
    parseExtraKeywords(String(extraKeywords || process.env.BSE_EXTRA_KEYWORDS || "")),
  );

  const payload = {
    strCat: String(category || process.env.BSE_CATEGORY || "-1"),
    strPrevDate: bseDate,
    strScrip: String(scrip || process.env.BSE_SCRIP || ""),
    strSearch: String(search || process.env.BSE_SEARCH || "P"),
    strToDate: bseDate,
    strType: String(annType || process.env.BSE_ANN_TYPE || "C"),
    subcategory: String(subcategory || process.env.BSE_SUBCATEGORY || ""),
  };
  const finalTimeoutMs = Number(timeoutMs || process.env.BSE_TIMEOUT_MS || 30000);
  const finalMaxPages = Number(maxPages || process.env.BSE_MAX_PAGES || 0);
  const maxPdfTextChars = Number(process.env.BSE_PDF_MAX_CHARS || 300000);

  try {
    await fetch("https://www.bseindia.com/", { headers: DEFAULT_HEADERS });
  } catch (_) {
    // best effort warmup
  }

  const rows = await iterAnnouncements({
    basePayload: payload,
    maxPages: finalMaxPages,
    timeoutMs: finalTimeoutMs,
  });
  const categorySet = new Set();

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || {};
    const newsId = cleanText(row?.NEWSID);
    if (!newsId) continue;
    const pdfUrl = extractPdfUrl(row);
    const metadataMatch = matcher.matchText(composeAnnouncementText(row));
    let pdfMatch = { score: 0, hits: {} };
    let matchSource = "";
    let pdfText = "";
    const hasPdf = Boolean(cleanText(pdfUrl));
    if (hasPdf) {
      try {
        const pdfBinary = await fetchBinary(pdfUrl, finalTimeoutMs);
        if (looksLikePdf(pdfBinary)) {
          pdfText = await extractPdfText(pdfBinary);
          pdfMatch = matcher.matchText(pdfText);
        } else {
          pdfText = "";
        }
      } catch (_) {
        pdfText = "";
      }
    }

    const mergedHits = mergeHits(metadataMatch.hits, pdfMatch.hits);
    const themes = Object.keys(mergedHits).sort();
    const score = Math.max(Number(metadataMatch.score || 0), Number(pdfMatch.score || 0));
    if (hasPdf && pdfMatch.score >= matchThreshold) {
      matchSource = "pdf";
    } else if (hasPdf && metadataMatch.score >= matchThreshold) {
      matchSource = "metadata";
    }
    const isMatched = Boolean(matchSource) && themes.length > 0 && score > 0;
    const storablePdfText =
      matchSource === "pdf" && pdfText ? String(pdfText).slice(0, Math.max(0, maxPdfTextChars)) : "";

    const categoryName = normalizeCategory(row?.CATEGORYNAME);
    if (categoryName) categorySet.add(categoryName);

    await db.query(
      `
        INSERT INTO bse_news (
          user_id, news_id, news_date, disseminated_at, scrip_code, company, headline, category,
          announcement_type, pdf_url, match_status, match_source, match_score, metadata_score, pdf_score, matched_themes, matched_terms, pdf_text,
          raw_data, fetched_for_date, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12, $13, $14, $15, $16::text[], $17::jsonb, $18,
          $19::jsonb, $20::date, NOW()
        )
        ON CONFLICT (news_id)
        DO UPDATE SET
          user_id = EXCLUDED.user_id,
          news_date = EXCLUDED.news_date,
          disseminated_at = EXCLUDED.disseminated_at,
          scrip_code = EXCLUDED.scrip_code,
          company = EXCLUDED.company,
          headline = EXCLUDED.headline,
          category = EXCLUDED.category,
          announcement_type = EXCLUDED.announcement_type,
          pdf_url = EXCLUDED.pdf_url,
          match_status = EXCLUDED.match_status,
          match_source = EXCLUDED.match_source,
          match_score = EXCLUDED.match_score,
          metadata_score = EXCLUDED.metadata_score,
          pdf_score = EXCLUDED.pdf_score,
          matched_themes = EXCLUDED.matched_themes,
          matched_terms = EXCLUDED.matched_terms,
          pdf_text = EXCLUDED.pdf_text,
          raw_data = EXCLUDED.raw_data,
          fetched_for_date = EXCLUDED.fetched_for_date,
          updated_at = NOW()
      `,
      [
        userId,
        newsId,
        cleanText(row?.NEWS_DT || row?.DT_TM),
        cleanText(row?.DissemDT),
        cleanText(row?.SCRIP_CD),
        cleanText(row?.SLONGNAME || row?.XML_NAME),
        cleanText(row?.HEADLINE),
        categoryName,
        cleanText(row?.ANNOUNCEMENT_TYPE),
        pdfUrl,
        isMatched ? "matched" : "unmatched",
        matchSource || null,
        score,
        Number(metadataMatch.score || 0),
        Number(pdfMatch.score || 0),
        themes,
        JSON.stringify(mergedHits || {}),
        storablePdfText,
        JSON.stringify(row || {}),
        requestDate,
      ],
    );
  }

  if (categorySet.size > 0) {
    const categories = Array.from(categorySet);
    for (let i = 0; i < categories.length; i += 1) {
      const categoryName = categories[i];
      await db.query(
        `
          INSERT INTO bse_news_categories (user_id, category_name, updated_at)
          VALUES ($1, $2, NOW())
          ON CONFLICT (user_id, category_name)
          DO UPDATE SET updated_at = NOW()
        `,
        [userId, categoryName],
      );
    }
  }

  const list = await getNewsList({
    userId,
    limit,
    offset,
    date: requestDate,
    category: "all",
    matchStatus: "matched",
  });
  return {
    fetchedForDate: requestDate,
    fetchedRows: rows.length,
    ...list,
  };
}

function mapVideoRow(row) {
  return {
    id: Number(row.id || 0),
    date: String(row.video_date || ""),
    category: String(row.scope_category || "all"),
    title: String(row.title || ""),
    renderJobId: String(row.render_job_id || ""),
    fileName: String(row.file_name || ""),
    videoUrl: String(row.video_url || ""),
    status: String(row.status || "completed"),
    totalNews: Number(row.total_news || 0),
    createdAt: row.created_at || null,
  };
}

async function createFullVideoRecord({
  userId,
  date,
  category,
  title,
  renderJobId,
  fileName,
  videoUrl,
  status = "completed",
  newsRows = [],
}) {
  await ensureSchema();
  const db = getPool();
  const videoDate = String(date || todayDateStr()).trim();
  const scopeCategory = normalizeCategory(category) || "all";
  const cleanNewsRows = Array.isArray(newsRows) ? newsRows : [];

  const upsert = await db.query(
    `
      INSERT INTO bse_full_videos (
        user_id, video_date, scope_category, title, render_job_id, file_name, video_url, status, total_news, updated_at
      ) VALUES (
        $1, $2::date, $3, $4, $5, $6, $7, $8, $9, NOW()
      )
      ON CONFLICT (user_id, render_job_id)
      DO UPDATE SET
        video_date = EXCLUDED.video_date,
        scope_category = EXCLUDED.scope_category,
        title = EXCLUDED.title,
        file_name = EXCLUDED.file_name,
        video_url = EXCLUDED.video_url,
        status = EXCLUDED.status,
        total_news = EXCLUDED.total_news,
        updated_at = NOW()
      RETURNING *
    `,
    [
      Number(userId || 0),
      videoDate,
      scopeCategory,
      String(title || ""),
      String(renderJobId || ""),
      String(fileName || ""),
      String(videoUrl || ""),
      String(status || "completed"),
      Number(cleanNewsRows.length || 0),
    ],
  );
  const video = upsert.rows[0];
  const videoId = Number(video?.id || 0);
  if (videoId <= 0) throw new Error("Failed to store full video record");

  await db.query(`DELETE FROM bse_full_video_news WHERE video_id = $1`, [videoId]);
  for (let i = 0; i < cleanNewsRows.length; i += 1) {
    const row = cleanNewsRows[i] || {};
    await db.query(
      `
        INSERT INTO bse_full_video_news (
          video_id, user_id, news_row_id, news_id, company, headline, category
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [
        videoId,
        Number(userId || 0),
        Number(row.newsRowId || 0),
        String(row.newsId || ""),
        String(row.company || ""),
        String(row.headline || ""),
        String(row.category || ""),
      ],
    );
  }
  return mapVideoRow(video);
}

async function listFullVideos({ userId, date, limit = 50, offset = 0 }) {
  await ensureSchema();
  const db = getPool();
  const videoDate = String(date || todayDateStr()).trim();
  const safeLimit = Math.min(200, Math.max(1, Number(limit || 50)));
  const safeOffset = Math.max(0, Number(offset || 0));

  const countRes = await db.query(
    `
      SELECT COUNT(*)::int AS total
      FROM bse_full_videos
      WHERE user_id = $1
        AND video_date = $2::date
    `,
    [Number(userId || 0), videoDate],
  );

  const rowsRes = await db.query(
    `
      SELECT *
      FROM bse_full_videos
      WHERE user_id = $1
        AND video_date = $2::date
      ORDER BY created_at DESC, id DESC
      LIMIT $3 OFFSET $4
    `,
    [Number(userId || 0), videoDate, safeLimit, safeOffset],
  );

  return {
    date: videoDate,
    totalRows: Number(countRes.rows[0]?.total || 0),
    limit: safeLimit,
    offset: safeOffset,
    rows: rowsRes.rows.map(mapVideoRow),
  };
}

module.exports = {
  fetchAndStoreNews,
  getNewsList,
  getNewsItem,
  getNewsCategories,
  updateNewsApproachData,
  resetNewsApproachData,
  getMatchedNewsForBatch,
  createNewsApproachJob,
  updateNewsApproachJob,
  getNewsApproachJob,
  requestStopNewsApproachJob,
  createFullVideoRecord,
  listFullVideos,
};
