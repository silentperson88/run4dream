const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);

module.exports = {
  toNumber,
  ensureArray,
};
