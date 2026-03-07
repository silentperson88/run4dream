function toBaseSymbol(input) {
  const value = String(input || "").trim().toUpperCase();
  if (!value) return "";

  const withoutExchange = value.split("#")[0];
  if (withoutExchange.endsWith("-EQ")) {
    return withoutExchange.slice(0, -3);
  }

  return withoutExchange;
}

function symbolCandidates(input) {
  const raw = String(input || "").trim().toUpperCase();
  const base = toBaseSymbol(raw);

  return Array.from(new Set([raw, base, base ? `${base}-EQ` : ""])).filter(
    Boolean,
  );
}

module.exports = {
  toBaseSymbol,
  symbolCandidates,
};
