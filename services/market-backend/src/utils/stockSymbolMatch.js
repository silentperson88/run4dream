const TRAILING_SYMBOL_SUFFIXES = [
  "OPTFUT",
  "OPTSTK",
  "EQ",
  "BE",
  "BZ",
  "SM",
  "SS",
  "ST",
  "XT",
  "X1",
  "NS",
  "BO",
  "OPTIDX",
  "OPTCUR",
  "OPTBLN",
  "OPTIRC",
  "FUTSTK",
  "COMDTY",
  "FUTCOM",
  "FUTCUR",
  "AMXIDX",
  "UNDIRC",
  "FUTBLN",
  "FUTIRC",
  "FUTBAS",
  "FUTIDX",
  "FUTENR",
  "UNDCUR",
  "INDEX",
  "FUTIRT",
  "UNDIRD",
  "UNDIRT",
];

const TRAILING_SYMBOL_SUFFIX_RE = new RegExp(
  `(?:[\\s._-]+)?(?:${TRAILING_SYMBOL_SUFFIXES.join("|")})$`,
  "i",
);

const TRAILING_SYMBOL_SUFFIX_PATTERN = `(?:[\\s._-]+)?(?:${TRAILING_SYMBOL_SUFFIXES.join("|")})$`;

const normalizeSymbolForMatch = (value) => {
  let symbol = String(value ?? "").trim().toUpperCase();
  if (!symbol) return "";

  symbol = symbol.split("#")[0];
  let previous;
  do {
    previous = symbol;
    symbol = symbol.replace(TRAILING_SYMBOL_SUFFIX_RE, "").trim();
  } while (symbol && symbol !== previous);
  symbol = symbol.replace(/[^A-Z0-9]+/g, "");
  return symbol;
};

const normalizeNameForMatch = (value) => {
  let name = String(value ?? "").trim().toUpperCase();
  if (!name) return "";

  name = name.split("#")[0];
  name = name.replace(/\bLIMITED\b/g, "LTD");
  name = name.replace(/[^A-Z0-9]+/g, "");
  return name;
};

module.exports = {
  normalizeSymbolForMatch,
  normalizeNameForMatch,
  TRAILING_SYMBOL_SUFFIX_PATTERN,
};
