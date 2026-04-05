const { analyzeScreenerHtmlRendered } = require("./screenerHtmlRendered.service");
const { buildMappedFundamentals } = require("./fundamentalsMapper.service");

const PRIMARY_FIELD_KEYS = ["market_cap", "current_price", "book_value"];

const normalizeScreenerUrl = (value) => String(value || "").trim().replace(/\/+$/g, "/");

const buildFallbackScreenerUrl = (url) => {
  const trimmed = normalizeScreenerUrl(url);
  if (!trimmed) return null;
  const fallback = trimmed.replace(/\/consolidated\/?$/i, "");
  return fallback === trimmed ? null : fallback;
};

const buildSecurityCodeScreenerUrl = (securityCode) => {
  const code = String(securityCode || "").trim();
  if (!code) return null;
  return `https://www.screener.in/company/${encodeURIComponent(code)}`;
};

const buildScreenerUrlCandidates = (url, securityCode, options = {}) => {
  const primaryUrl = normalizeScreenerUrl(url);
  const candidates = [];
  const securityCodeUrl = buildSecurityCodeScreenerUrl(securityCode);

  if (options?.preferSecurityCodeFirst && securityCodeUrl) {
    candidates.push(securityCodeUrl);
  }

  if (primaryUrl) {
    if (!options?.securityCodeOnly) {
      candidates.push(primaryUrl);
      const fallbackUrl = buildFallbackScreenerUrl(primaryUrl);
      if (fallbackUrl) candidates.push(fallbackUrl);
    }
  }

  if (!options?.preferSecurityCodeFirst && securityCodeUrl) {
    candidates.push(securityCodeUrl);
  }

  return Array.from(new Set(candidates.filter(Boolean)));
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
  const candidates = buildScreenerUrlCandidates(url, options?.securityCode, options);
  const primaryUrl = candidates[0] || "";
  if (!primaryUrl) {
    throw new Error("Missing screener_url");
  }

  const terminalFailure = async (failedUrl, reason) => {
    const error = new Error(reason);
    error.attempts = attempts;
    error.failedUrl = failedUrl || null;
    throw error;
  };

  let lastError = null;
  for (let i = 0; i < candidates.length; i += 1) {
    const candidateUrl = candidates[i];
    try {
      const candidateData = await analyzeScreenerHtmlRendered(candidateUrl, options);
      const candidateSnapshot = getPrimarySnapshot(candidateData);
      const candidateValidation = validatePrimarySnapshot(candidateSnapshot);
      attempts.push({
        url: candidateUrl,
        valid: candidateValidation.valid,
        failedFields: candidateValidation.failedFields,
      });

      if (!candidateValidation.valid || !hasUsableFundamentals(candidateData)) {
        lastError = new Error(
          `Primary fundamentals missing on ${candidateUrl} (${candidateValidation.failedFields.join(", ")})`,
        );
        lastError.failedFields = candidateValidation.failedFields;
        lastError.attempts = attempts;
        lastError.selectedUrl = candidateUrl;
        if (options?.strictPendingFlow) {
          return terminalFailure(candidateUrl, lastError.message);
        }
        continue;
      }

      return {
        data: candidateData,
        selectedUrl: candidateUrl,
        fallbackUsed: i > 0,
        attempts,
        primaryValidation: candidateValidation,
      };
    } catch (err) {
      lastError = err;
      attempts.push({
        url: candidateUrl,
        valid: false,
        failedFields: Array.isArray(err?.failedFields) ? err.failedFields : [],
      });
      lastError.attempts = attempts;
      lastError.selectedUrl = candidateUrl;
    }
  }

  if (lastError) throw lastError;
  const error = new Error("Unable to fetch screener fundamentals");
  error.attempts = attempts;
  throw error;
};

module.exports = {
  PRIMARY_FIELD_KEYS,
  buildFallbackScreenerUrl,
  buildSecurityCodeScreenerUrl,
  buildScreenerUrlCandidates,
  getPrimarySnapshot,
  validatePrimarySnapshot,
  scrapeWithFallback,
  hasUsableFundamentals,
};
