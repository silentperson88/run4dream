const GOOGLE_SEARCH_URL = "https://www.google.com/search";

const DEFAULT_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-IN,en;q=0.9",
  "Cache-Control": "no-cache",
  Pragma: "no-cache",
};

const decodeHtmlEntities = (text = "") =>
  String(text)
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16)))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");

const stripHtml = (html = "") =>
  decodeHtmlEntities(
    String(html)
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
  );

function findAiContainerHtml(html = "") {
  const startMarker = 'data-container-id="main-col"';
  const xidMarker = 'data-xid="VpUvz"';
  const jsNameMarker = 'jsname="KFl8ub"';

  const anchorIndex = html.indexOf(startMarker);
  if (anchorIndex < 0) return null;

  const searchWindow = html.slice(Math.max(0, anchorIndex - 500), anchorIndex + 1000);
  if (!searchWindow.includes(xidMarker) || !searchWindow.includes(jsNameMarker)) return null;

  const divStart = html.lastIndexOf("<div", anchorIndex);
  if (divStart < 0) return null;

  let depth = 0;
  const tagPattern = /<\/?div\b[^>]*>/gi;
  tagPattern.lastIndex = divStart;

  let match;
  while ((match = tagPattern.exec(html))) {
    const token = match[0];
    if (token.startsWith("</div")) {
      depth -= 1;
      if (depth === 0) {
        return html.slice(divStart, tagPattern.lastIndex);
      }
    } else {
      depth += 1;
    }
  }

  return null;
}

function extractSourceHints(containerHtml = "") {
  const hints = [];
  const regex = /"https?:\/\/[^"]+"/g;
  const seen = new Set();
  const matches = containerHtml.match(regex) || [];

  for (const raw of matches) {
    const url = decodeHtmlEntities(raw.slice(1, -1));
    if (!/^https?:\/\//i.test(url)) continue;
    if (url.includes("google.com/search/about-this-result")) continue;
    if (seen.has(url)) continue;
    seen.add(url);
    hints.push(url);
    if (hints.length >= 8) break;
  }

  return hints;
}

async function scrapeGoogleAiAnswer({ question }) {
  const query = String(question || "").trim();
  if (!query) throw new Error("question is required");

  const url = new URL(GOOGLE_SEARCH_URL);
  url.searchParams.set("q", query);
  url.searchParams.set("hl", "en-IN");
  url.searchParams.set("gl", "IN");

  const res = await fetch(url, {
    headers: DEFAULT_HEADERS,
  });

  const html = await res.text();
  if (!res.ok) {
    throw new Error(`Google search request failed with status ${res.status}`);
  }

  const containerHtml = findAiContainerHtml(html);
  if (!containerHtml) {
    return {
      query,
      answer: "",
      found: false,
      sources: [],
      note: "AI answer container not found in the Google response HTML.",
      html,
    };
  }

  return {
    query,
    answer: stripHtml(containerHtml),
    found: true,
    sources: extractSourceHints(containerHtml),
    note: "",
    html,
  };
}

module.exports = {
  scrapeGoogleAiAnswer,
};
