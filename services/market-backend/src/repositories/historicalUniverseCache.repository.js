const crypto = require("crypto");

const cacheStore = new Map();

const stableStringify = (value) => {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }
  if (typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
};

const normalizeDateOnly = (value) => {
  if (!value) return null;
  return String(value).slice(0, 10);
};

const buildHistoricalUniverseCacheKey = ({ cacheType = "filter", asOfDate = null, rules = {}, query = "" } = {}) => {
  const normalizedAsOfDate = normalizeDateOnly(asOfDate);
  const rulesHash = crypto.createHash("sha1").update(stableStringify(rules || {})).digest("hex");
  const queryHash = crypto.createHash("sha1").update(String(query || "").trim()).digest("hex");
  const cacheKey = [cacheType, normalizedAsOfDate || "all", rulesHash, queryHash].join(":");

  return {
    cacheKey,
    cacheType,
    asOfDate: normalizedAsOfDate,
    rulesHash,
    queryHash,
  };
};

const getHistoricalUniverseCache = async ({ cacheType = "filter", asOfDate = null, rules = {}, query = "" } = {}) => {
  const keyInfo = buildHistoricalUniverseCacheKey({ cacheType, asOfDate, rules, query });
  return cacheStore.get(keyInfo.cacheKey) || null;
};

const upsertHistoricalUniverseCache = async ({
  cacheKey,
  cacheType = "filter",
  asOfDate = null,
  rulesHash = null,
  queryHash = null,
  engine = null,
  payload = null,
} = {}) => {
  if (!cacheKey) return null;

  const record = {
    cache_key: cacheKey,
    cache_type: cacheType,
    as_of_date: normalizeDateOnly(asOfDate),
    rules_hash: rulesHash,
    query_hash: queryHash,
    engine,
    payload,
    updated_at: new Date().toISOString(),
  };

  cacheStore.set(cacheKey, record);
  return record;
};

module.exports = {
  buildHistoricalUniverseCacheKey,
  getHistoricalUniverseCache,
  upsertHistoricalUniverseCache,
};
