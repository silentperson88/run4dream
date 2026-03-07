const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const toNullableNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);
const ensureObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

module.exports = {
  toNumber,
  toNullableNumber,
  ensureArray,
  ensureObject,
};
