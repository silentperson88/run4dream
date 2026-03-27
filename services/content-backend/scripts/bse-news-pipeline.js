/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { chatWithOllama } = require("../src/services/ollama.service");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

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

function argValue(name, fallback = "") {
  const prefix = `--${name}=`;
  const found = process.argv.find((a) => a.startsWith(prefix));
  return found ? found.slice(prefix.length).trim() : fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function todayDateStr() {
  const d = new Date();
  const yyyy = String(d.getFullYear());
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function parseDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return new Date(todayDateStr());
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return new Date(`${raw}T00:00:00`);
  if (/^\d{8}$/.test(raw)) {
    return new Date(`${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T00:00:00`);
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) throw new Error(`Unsupported date format: ${raw}`);
  return parsed;
}

function toBseDate(dateObj) {
  const yyyy = String(dateObj.getFullYear());
  const mm = String(dateObj.getMonth() + 1).padStart(2, "0");
  const dd = String(dateObj.getDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function cleanText(value) {
  return String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function slugify(value, maxLen = 90) {
  const text = cleanText(value)
    .normalize("NFKD")
    .replace(/[^\x00-\x7F]/g, "")
    .replace(/[^A-Za-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
  const out = text || "announcement";
  return out.slice(0, maxLen).replace(/-+$/g, "");
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function uniquePath(filePath) {
  if (!fs.existsSync(filePath)) return filePath;
  const ext = path.extname(filePath);
  const base = filePath.slice(0, -ext.length);
  for (let i = 1; i < 10000; i += 1) {
    const candidate = `${base}_${i}${ext}`;
    if (!fs.existsSync(candidate)) return candidate;
  }
  throw new Error(`Unable to find unique filename for ${filePath}`);
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

async function iterAnnouncements({ basePayload, maxPages, timeoutMs, requestDelayMs, verbose }) {
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
    if (!table.length) {
      if (verbose) console.log(`No rows at page ${page}, stopping.`);
      break;
    }

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
    if (requestDelayMs > 0) await sleep(requestDelayMs);
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

function looksLikePdf(buf) {
  const trimmed = Buffer.from(buf).slice(0, 8).toString("utf8");
  return trimmed.includes("%PDF");
}

function destinationFor(row, pdfUrl, pdfDir) {
  const parsed = new URL(pdfUrl);
  const baseName = path.basename(parsed.pathname) || "file.pdf";
  const finalBaseName = baseName.toLowerCase().endsWith(".pdf") ? baseName : `${baseName}.pdf`;
  const dateValue = cleanText(row?.NEWS_DT || row?.DT_TM);
  const dateSlug = slugify(dateValue, 20);
  const scripCode = cleanText(row?.SCRIP_CD) || "unknown";
  const company = cleanText(row?.SLONGNAME || row?.XML_NAME);
  const companySlug = slugify(company, 55);
  const fileName = `${dateSlug}_${scripCode}_${companySlug}_${finalBaseName}`.slice(0, 220);
  let target = path.join(pdfDir, fileName);
  if (!target.toLowerCase().endsWith(".pdf")) target += ".pdf";
  return uniquePath(target);
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

function csvEscape(v) {
  const s = String(v ?? "");
  if (!/[",\n]/.test(s)) return s;
  return `"${s.replace(/"/g, "\"\"")}"`;
}

function writeCsv(filePath, rows, fields) {
  const lines = [fields.join(",")];
  rows.forEach((row) => {
    lines.push(fields.map((f) => csvEscape(row?.[f] ?? "")).join(","));
  });
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
}

function buildSummaryPrompt(entry) {
  return `
Summarize the following corporate announcement metadata in a strict factual style.

Instructions:
- Keep summary concise and factual.
- Do NOT add assumptions.
- Mention if details are unclear/missing.

Output format:
Document Summary

Filing Type:
Company:
Main Events:
Key Dates:
Key Numbers:
Regulatory Mentions:
Sentiment Analysis:
Unclear / Missing:

Metadata:
- News ID: ${entry.news_id}
- News Date: ${entry.news_date}
- Company: ${entry.company}
- Category: ${entry.category}
- Announcement Type: ${entry.announcement_type}
- Headline: ${entry.headline}
- Matched Themes: ${entry.matched_themes}
- PDF URL: ${entry.pdf_url}
`.trim();
}

async function summarizeEntries(entries, outDir) {
  const results = [];
  for (let i = 0; i < entries.length; i += 1) {
    const entry = entries[i];
    try {
      const res = await chatWithOllama({
        text: buildSummaryPrompt(entry),
        options: { temperature: 0.1 },
      });
      const summaryText = String(res?.response || "").trim();
      results.push({
        news_id: entry.news_id,
        file_name: path.basename(entry.local_file || ""),
        summary: summaryText,
      });
      const safeName = slugify(path.basename(entry.local_file || entry.news_id || `item-${i + 1}`), 180);
      fs.writeFileSync(path.join(outDir, `${safeName}.txt`), summaryText, "utf8");
    } catch (err) {
      results.push({
        news_id: entry.news_id,
        file_name: path.basename(entry.local_file || ""),
        error: err?.message || String(err),
      });
    }
  }
  fs.writeFileSync(path.join(outDir, "announcement_summaries.json"), JSON.stringify(results, null, 2), "utf8");
}

async function main() {
  const startDateRaw = argValue("start-date", process.env.BSE_START_DATE || "");
  const endDateRaw = argValue("end-date", process.env.BSE_END_DATE || "");
  const today = todayDateStr();
  const startDate = startDateRaw || today;
  const endDate = endDateRaw || today;
  const outputDir = path.resolve(__dirname, "..", argValue("output-dir", process.env.BSE_OUTPUT_DIR || "generated-news"));
  const category = argValue("category", process.env.BSE_CATEGORY || "-1");
  const scrip = argValue("scrip", process.env.BSE_SCRIP || "");
  const search = argValue("search", process.env.BSE_SEARCH || "P");
  const annType = argValue("ann-type", process.env.BSE_ANN_TYPE || "C");
  const subcategory = argValue("subcategory", process.env.BSE_SUBCATEGORY || "");
  const extraKeywords = argValue("extra-keywords", process.env.BSE_EXTRA_KEYWORDS || "");
  const minScore = Number(argValue("min-score", process.env.BSE_MIN_SCORE || "1"));
  const maxPages = Number(argValue("max-pages", process.env.BSE_MAX_PAGES || "0"));
  const timeoutMs = Number(argValue("timeout-ms", process.env.BSE_TIMEOUT_MS || "30000"));
  const requestDelayMs = Number(argValue("delay-ms", process.env.BSE_REQUEST_DELAY_MS || "150"));
  const verbose = String(argValue("verbose", process.env.BSE_VERBOSE || "true")).toLowerCase() === "true";

  const startDt = parseDate(startDate);
  const endDt = parseDate(endDate);
  if (startDt.getTime() > endDt.getTime()) throw new Error("start-date must be <= end-date");

  const pdfDir = path.join(outputDir, "pdfs");
  const summaryDir = path.join(outputDir, "pdf_summaries", "by_pdf");
  ensureDir(outputDir);
  ensureDir(pdfDir);
  ensureDir(summaryDir);

  try {
    await fetch("https://www.bseindia.com/", { headers: DEFAULT_HEADERS });
  } catch (_) {
    // best effort
  }

  const matcher = new SemanticMatcher(parseExtraKeywords(extraKeywords));
  const basePayload = {
    strCat: category,
    strPrevDate: toBseDate(startDt),
    strScrip: scrip,
    strSearch: search,
    strToDate: toBseDate(endDt),
    strType: annType,
    subcategory,
  };

  const stats = { scanned: 0, with_pdf: 0, matched: 0, downloaded: 0, errors: 0 };
  const failures = [];
  const outputRows = [];

  const rows = await iterAnnouncements({ basePayload, maxPages, timeoutMs, requestDelayMs, verbose });
  for (const row of rows) {
    stats.scanned += 1;
    const pdfUrl = extractPdfUrl(row);
    if (!pdfUrl) continue;
    stats.with_pdf += 1;

    const metadataText = composeAnnouncementText(row);
    const metadataMatch = matcher.matchText(metadataText);
    if (metadataMatch.score < minScore) continue;

    stats.matched += 1;
    try {
      const pdfContent = await fetchBinary(pdfUrl, timeoutMs);
      if (!looksLikePdf(pdfContent)) throw new Error("Downloaded content is not a PDF");

      const destPath = destinationFor(row, pdfUrl, pdfDir);
      fs.writeFileSync(destPath, pdfContent);
      stats.downloaded += 1;

      const combinedHits = mergeHits(metadataMatch.hits, {});
      outputRows.push({
        news_id: cleanText(row?.NEWSID),
        news_date: cleanText(row?.NEWS_DT || row?.DT_TM),
        disseminated_at: cleanText(row?.DissemDT),
        scrip_code: cleanText(row?.SCRIP_CD),
        company: cleanText(row?.SLONGNAME || row?.XML_NAME),
        headline: cleanText(row?.HEADLINE),
        category: cleanText(row?.CATEGORYNAME),
        announcement_type: cleanText(row?.ANNOUNCEMENT_TYPE),
        pdf_url: pdfUrl,
        local_file: destPath,
        match_source: "metadata",
        metadata_score: metadataMatch.score,
        pdf_score: 0,
        matched_themes: Object.keys(combinedHits).sort().join(", "),
        matched_terms: JSON.stringify(combinedHits),
      });
      if (verbose) console.log(`Matched and downloaded: ${cleanText(row?.NEWSID)} -> ${path.basename(destPath)}`);
    } catch (err) {
      stats.errors += 1;
      failures.push({
        news_id: cleanText(row?.NEWSID),
        pdf_url: pdfUrl,
        error: `download_failed: ${err?.message || String(err)}`,
      });
    }
  }

  const csvPath = path.join(outputDir, "matched_announcements.csv");
  const jsonPath = path.join(outputDir, "matched_announcements.json");
  const failuresPath = path.join(outputDir, "failures.json");
  const statsPath = path.join(outputDir, "summary.json");
  writeCsv(csvPath, outputRows, [
    "news_id",
    "news_date",
    "disseminated_at",
    "scrip_code",
    "company",
    "headline",
    "category",
    "announcement_type",
    "pdf_url",
    "local_file",
    "match_source",
    "metadata_score",
    "pdf_score",
    "matched_themes",
    "matched_terms",
  ]);
  fs.writeFileSync(jsonPath, JSON.stringify(outputRows, null, 2), "utf8");
  fs.writeFileSync(failuresPath, JSON.stringify(failures, null, 2), "utf8");
  fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2), "utf8");

  await summarizeEntries(outputRows, summaryDir);

  console.log("Scan complete");
  console.log(`Date window: ${startDate} -> ${endDate}`);
  console.log(`Scanned announcements: ${stats.scanned}`);
  console.log(`Announcements with PDF: ${stats.with_pdf}`);
  console.log(`Matched announcements: ${stats.matched}`);
  console.log(`PDFs downloaded: ${stats.downloaded}`);
  console.log(`Errors: ${stats.errors}`);
  console.log(`CSV report: ${csvPath}`);
  console.log(`JSON report: ${jsonPath}`);
  console.log(`Failures report: ${failuresPath}`);
  console.log(`Summary: ${statsPath}`);
  console.log(`Per-file summaries: ${summaryDir}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
