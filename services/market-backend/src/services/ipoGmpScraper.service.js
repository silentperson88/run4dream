const axios = require("axios");
const cheerio = require("cheerio");
const { launchChromium } = require("../utils/browserLauncher");

const LIVE_IPO_GMP_URL = "https://www.investorgain.com/report/live-ipo-gmp/331/";

const normalizeText = (value) =>
  String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeHeader = (value) =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

const sanitizeValue = (value) => {
  const cleaned = normalizeText(value);
  if (!cleaned || cleaned === "-" || cleaned === "--" || cleaned === "N/A") {
    return null;
  }
  return cleaned;
};

const fieldFromRow = (rowEntries, patterns = []) => {
  const match = rowEntries.find(({ key, normalizedKey }) =>
    patterns.some((pattern) => pattern.test(normalizedKey) || pattern.test(key)),
  );
  return match ? sanitizeValue(match.value) : null;
};

const extractTextFromCell = ($, cellEl) => {
  const linkText = sanitizeValue($(cellEl).find("a").first().text());
  if (linkText) return linkText;
  return sanitizeValue($(cellEl).text());
};

const parseGmpBreakdown = ($, tdEls = [], rowEntries = []) => {
  const gmpCell = tdEls.find((cell) => normalizeHeader($(cell).attr("data-label")) === "gmp");
  if (!gmpCell) {
    return {
      gain_price: fieldFromRow(rowEntries, [/^gmp$/, /grey.*market.*premium/]),
      gain_percentage: null,
    };
  }

  const gmpText = normalizeText($(gmpCell).text());
  const bTags = $(gmpCell)
    .find("b")
    .toArray()
    .map((el) => sanitizeValue($(el).text()))
    .filter(Boolean);

  const currentRaw = bTags.length ? bTags[0] : null;
  const gainPrice = currentRaw === "--" ? null : sanitizeValue(currentRaw);
  const gainMatch = gmpText.match(/\(([-+]?\d+(?:\.\d+)?)%\)/);
  const gainPercentage = gainMatch ? sanitizeValue(gainMatch[1]) : null;

  return { gain_price: gainPrice, gain_percentage: gainPercentage };
};

const parseStatusCode = (badgeText) => {
  const value = normalizeText(badgeText).toUpperCase();
  if (!value) return "L";
  if (value.includes("CL") || value.includes("CLOSE")) return "CL";
  if (value.includes("U")) return "U";
  if (value.includes("O")) return "O";
  return "L";
};

const parseRatingCount = ($, tdEls = []) => {
  const ratingCell = tdEls.find((cell) => normalizeHeader($(cell).attr("data-label")) === "rating");
  if (!ratingCell) return 0;

  const ratingText = normalizeText($(ratingCell).text());
  const emojiCount = (ratingText.match(/🔥/g) || []).length;
  const iconCount = $(ratingCell).find("[class*='fire'], [class*='flame']").length;
  return emojiCount + iconCount;
};

const parseInstitutionalBacking = ($, tdEls = [], rowEntries = []) => {
  const anchorCell = tdEls.find((cell) => normalizeHeader($(cell).attr("data-label")) === "anchor");
  const anchorValue =
    (anchorCell ? sanitizeValue($(anchorCell).text()) : null) ||
    fieldFromRow(rowEntries, [/anchor/]) ||
    "";
  return anchorValue.includes("✅") ? 1 : 0;
};

const MONTH_MAP = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
};

const normalizeDateToDayMonthNumber = (rawDate) => {
  const text = normalizeText(rawDate);
  const match = text.match(/(\d{1,2})\s*-\s*([A-Za-z]{3,9})/);
  if (!match) return sanitizeValue(text);

  const day = String(parseInt(match[1], 10));
  const monthKey = match[2].slice(0, 3).toLowerCase();
  const monthNum = MONTH_MAP[monthKey];
  if (!monthNum) return sanitizeValue(text);
  return `${day}-${monthNum}`;
};

const parseDateAndGmpCell = ($, tdEls = [], label) => {
  const target = tdEls.find(
    (cell) => normalizeHeader($(cell).attr("data-label")) === normalizeHeader(label),
  );
  if (!target) return { date: null, gmp: null };

  const fullText = normalizeText($(target).text());
  const dateMatch = fullText.match(/(\d{1,2}\s*-\s*[A-Za-z]{3,9})/);
  const gmpMatch = fullText.match(/GMP\s*:\s*([-+]?\d+(?:\.\d+)?|--)/i);

  return {
    date: dateMatch ? normalizeDateToDayMonthNumber(dateMatch[1]) : null,
    gmp: gmpMatch && gmpMatch[1] !== "--" ? sanitizeValue(gmpMatch[1]) : null,
  };
};

const parseReportTable = (html) => {
  const $ = cheerio.load(html);
  const table = $("#report_table").first();
  if (!table.length) {
    throw new Error("report_table id not found in source HTML");
  }

  const rows = [];
  table.find("tbody tr").each((_, rowEl) => {
    const tdEls = $(rowEl).find("td").toArray();
    if (!tdEls.length) return;

    const rawRow = {};
    tdEls.forEach((cellEl, idx) => {
      const dataLabel = sanitizeValue($(cellEl).attr("data-label"));
      const key = dataLabel || `col_${idx + 1}`;
      rawRow[key] = extractTextFromCell($, cellEl);
    });

    const rowEntries = Object.entries(rawRow).map(([key, value]) => ({
      key,
      normalizedKey: normalizeHeader(key),
      value,
    }));

    const nameCell = tdEls[0];
    const nameFromAnchor = nameCell
      ? sanitizeValue($(nameCell).find("a").first().text())
      : null;
    const badgeText = nameCell
      ? sanitizeValue($(nameCell).find(".badge").first().text())
      : null;

    const name =
      nameFromAnchor ||
      fieldFromRow(rowEntries, [/^name$/, /ipo.*name/, /company/]);

    if (!name) return;

    const gmpMeta = parseGmpBreakdown($, tdEls, rowEntries);
    const openMeta = parseDateAndGmpCell($, tdEls, "Open");
    const closeMeta = parseDateAndGmpCell($, tdEls, "Close");
    const listingMeta = parseDateAndGmpCell($, tdEls, "Listing");
    const type = /SME\s*$/i.test(name) ? "SME" : "MAINBOARD";

    rows.push({
      name,
      status: parseStatusCode(badgeText),
      gain_price: gmpMeta.gain_price,
      gain_percentage: gmpMeta.gain_percentage,
      ai_score: null,
      rating: parseRatingCount($, tdEls),
      subscribed: fieldFromRow(rowEntries, [/^sub$/, /subscription/, /subscribed/, /sub.*times/]),
      price: fieldFromRow(rowEntries, [/^price$/, /^price_/, /price/]),
      ipo_size: fieldFromRow(rowEntries, [/ipo.*size/]),
      lot: fieldFromRow(rowEntries, [/^lot$/]),
      open_date: openMeta.date || normalizeDateToDayMonthNumber(fieldFromRow(rowEntries, [/^open$/])),
      open_gmp: openMeta.gmp,
      close_date: closeMeta.date || normalizeDateToDayMonthNumber(fieldFromRow(rowEntries, [/^close$/])),
      close_gmp: closeMeta.gmp,
      boarding_date: normalizeDateToDayMonthNumber(fieldFromRow(rowEntries, [/boa/, /boarding/, /allotment/])),
      listing_date:
        listingMeta.date ||
        normalizeDateToDayMonthNumber(fieldFromRow(rowEntries, [/listing.*date/, /^listing$/])),
      listing_gmp: listingMeta.gmp,
      institutional_backing: parseInstitutionalBacking($, tdEls, rowEntries),
      type,
    });
  });

  return rows;
};

const hasReportTable = (html) => {
  const $ = cheerio.load(html || "");
  return $("#report_table").length > 0;
};

const fetchHtmlViaAxios = async (url) => {
  const { data } = await axios.get(url, {
    timeout: 30000,
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });
  return String(data || "");
};

const fetchHtmlViaPlaywright = async (url) => {
  const browser = await launchChromium();
  try {
    const page = await browser.newPage({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      locale: "en-US",
    });
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForSelector("#report_table", { timeout: 20000 });
    return await page.content();
  } finally {
    await browser.close();
  }
};

const scrapeLiveIpoGmpRows = async (url = LIVE_IPO_GMP_URL) => {
  let html = await fetchHtmlViaAxios(url);
  if (!hasReportTable(html)) {
    html = await fetchHtmlViaPlaywright(url);
  }
  return parseReportTable(html);
};

module.exports = {
  LIVE_IPO_GMP_URL,
  scrapeLiveIpoGmpRows,
};
