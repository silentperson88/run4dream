const { analyzeScreenerHtmlRendered } = require("./screenerHtmlRendered.service");
const { buildMappedFundamentals } = require("./fundamentalsMapper.service");

const PRIMARY_FIELD_KEYS = ["market_cap", "current_price", "book_value"];

const buildFallbackScreenerUrl = (url) => {
  if (!url || typeof url !== "string") return null;
  const trimmed = url.trim();
  if (!trimmed) return null;
  const fallback = trimmed.replace(/\/consolidated\/?$/i, "/");
  return fallback === trimmed ? null : fallback;
};

const isValidPrimaryNumber = (value, { allowZero = false } = {}) => {
  if (value === null || value === undefined) return false;
  if (typeof value !== "number" || !Number.isFinite(value)) return false;
  return allowZero ? value >= 0 : value > 0;
};

const getPrimarySnapshot = (rawData) =>
  buildMappedFundamentals(rawData)?.summary?.market_snapshot || {};

const hasUsableFundamentals = (data) => {
  if (!data || typeof data !== "object") return false;
  if (data?.company_info?.company_name) return true;

  const mapped = buildMappedFundamentals(data);
  const sections = [
    mapped?.peers?.main_table?.rows,
    mapped?.tables?.quarters?.rows,
    mapped?.tables?.profit_loss?.rows,
    mapped?.tables?.balance_sheet?.rows,
    mapped?.tables?.cash_flow?.rows,
    mapped?.tables?.ratios?.rows,
    mapped?.tables?.shareholdings?.rows,
  ];
  return sections.some((rows) => Array.isArray(rows) && rows.length > 0);
};

const validatePrimarySnapshot = (snapshot = {}) => {
  const failedFields = [];

  if (!isValidPrimaryNumber(snapshot.market_cap)) {
    failedFields.push("market_cap");
  }
  if (!isValidPrimaryNumber(snapshot.current_price)) {
    failedFields.push("current_price");
  }
  if (snapshot.book_value === null || snapshot.book_value === undefined) {
    failedFields.push("book_value");
  }

  return {
    valid: failedFields.length === 0,
    failedFields,
  };
};

const scrapeWithFallback = async (url, options = {}) => {
  const attempts = [];
  const primaryUrl = url || "";
  if (!primaryUrl) {
    throw new Error("Missing screener_url");
  }

  const primaryData = await analyzeScreenerHtmlRendered(primaryUrl, options);
  const primarySnapshot = getPrimarySnapshot(primaryData);
  const primaryValidation = validatePrimarySnapshot(primarySnapshot);
  attempts.push({
    url: primaryUrl,
    valid: primaryValidation.valid,
    failedFields: primaryValidation.failedFields,
  });

  if (primaryValidation.valid) {
    if (!hasUsableFundamentals(primaryData)) {
      const error = new Error("Empty/invalid fundamentals extracted from screener");
      error.failedFields = primaryValidation.failedFields;
      error.attempts = attempts;
      throw error;
    }
    return {
      data: primaryData,
      selectedUrl: primaryUrl,
      fallbackUsed: false,
      attempts,
      primaryValidation,
    };
  }

  const fallbackUrl = buildFallbackScreenerUrl(primaryUrl);
  if (!fallbackUrl) {
    const error = new Error(
      `Primary fundamentals missing on ${primaryUrl} (${primaryValidation.failedFields.join(", ")})`,
    );
    error.failedFields = primaryValidation.failedFields;
    error.attempts = attempts;
    throw error;
  }

  const fallbackData = await analyzeScreenerHtmlRendered(fallbackUrl, options);
  const fallbackSnapshot = getPrimarySnapshot(fallbackData);
  const fallbackValidation = validatePrimarySnapshot(fallbackSnapshot);
  attempts.push({
    url: fallbackUrl,
    valid: fallbackValidation.valid,
    failedFields: fallbackValidation.failedFields,
  });

  if (!fallbackValidation.valid) {
    const error = new Error(
      `Primary fundamentals missing on fallback URL too (${fallbackValidation.failedFields.join(", ")})`,
    );
    error.failedFields = fallbackValidation.failedFields;
    error.attempts = attempts;
    error.primaryUrl = primaryUrl;
    error.fallbackUrl = fallbackUrl;
    throw error;
  }

  if (!hasUsableFundamentals(fallbackData)) {
    const error = new Error("Empty/invalid fundamentals extracted from fallback screener");
    error.failedFields = fallbackValidation.failedFields;
    error.attempts = attempts;
    error.primaryUrl = primaryUrl;
    error.fallbackUrl = fallbackUrl;
    throw error;
  }

  return {
    data: fallbackData,
    selectedUrl: fallbackUrl,
    fallbackUsed: true,
    attempts,
    primaryValidation,
    fallbackValidation,
  };
};

module.exports = {
  PRIMARY_FIELD_KEYS,
  buildFallbackScreenerUrl,
  getPrimarySnapshot,
  validatePrimarySnapshot,
  scrapeWithFallback,
  hasUsableFundamentals,
};
