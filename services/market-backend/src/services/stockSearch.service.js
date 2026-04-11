const { pool } = require("../config/db");
const { buildValueAnalysisRows } = require("./valueAnalysis.service");

const SEARCH_FIELDS = [
  {
    key: "symbol",
    label: "Symbol",
    aliases: ["symbol", "ticker", "code"],
    example: "symbol = TATAMOTORS",
    value: (row) => row?.symbol || null,
    type: "text",
  },
  {
    key: "company_name",
    label: "Company Name",
    aliases: ["company", "name", "company name"],
    example: "company contains Tata",
    value: (row) => row?.company_name || row?.name || null,
    type: "text",
  },
  {
    key: "promoters",
    label: "Promoter Holding",
    aliases: ["promoter", "promoters", "promoter holding", "holding"],
    example: "promoter > 70",
    value: (row) => row?.value_metrics?.promoters ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "promoter_net_change_4q",
    label: "Promoter Net 4Q Change",
    aliases: ["promoter trend", "promoter net", "promoter 4q", "promoter change"],
    example: "promoter trend > 0",
    value: (row) => row?.analysis?.metrics?.promoter_net_change_4q ?? null,
    type: "number",
    unit: "pp",
  },
  {
    key: "promoter_max_quarter_drop_4q",
    label: "Promoter Max Quarter Drop",
    aliases: ["promoter drop", "quarter drop", "promoter quarter drop"],
    example: "promoter drop < 3",
    value: (row) => row?.analysis?.metrics?.promoter_max_quarter_drop_4q ?? null,
    type: "number",
    unit: "pp",
  },
  {
    key: "fiis",
    label: "FII Holding",
    aliases: ["fii", "fii holding", "foreign holding", "institutional"],
    example: "fii > 10",
    value: (row) => row?.value_metrics?.fiis ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "dii",
    label: "DII Holding",
    aliases: ["dii", "dii holding", "domestic holding"],
    example: "dii > 5",
    value: (row) => row?.value_metrics?.diis ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "public",
    label: "Public Holding",
    aliases: ["public", "public holding", "retail"],
    example: "public < 25",
    value: (row) => row?.value_metrics?.public ?? row?.analysis?.metrics?.public_net_change_4q ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "roe",
    label: "ROE",
    aliases: ["roe", "return on equity"],
    example: "roe > 15",
    value: (row) => row?.value_metrics?.roe ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "roce",
    label: "ROCE",
    aliases: ["roce", "return on capital employed"],
    example: "roce > 15",
    value: (row) => row?.value_metrics?.roce ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "debt_to_equity",
    label: "Debt to Equity",
    aliases: ["debt", "debt equity", "debt to equity", "d/e"],
    example: "debt to equity < 1",
    value: (row) => row?.value_metrics?.debt_to_equity ?? null,
    type: "number",
  },
  {
    key: "company_age_years",
    label: "Company Age",
    aliases: ["age", "company age", "listed years"],
    example: "age > 5",
    value: (row) => row?.value_metrics?.company_age_years ?? null,
    type: "number",
    unit: "years",
  },
  {
    key: "revenue_cagr_3y",
    label: "Revenue CAGR 3Y",
    aliases: ["revenue", "sales growth", "sales cagr", "revenue cagr"],
    example: "revenue > 15",
    value: (row) => row?.value_metrics?.revenue_cagr_3y ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "profit_cagr_3y",
    label: "Profit CAGR 3Y",
    aliases: ["profit growth", "profit cagr", "earnings growth"],
    example: "profit > 20",
    value: (row) => row?.value_metrics?.profit_cagr_3y ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "eps_cagr_3y",
    label: "EPS CAGR 3Y",
    aliases: ["eps", "eps growth"],
    example: "eps > 15",
    value: (row) => row?.value_metrics?.eps_cagr_3y ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "opm_percent",
    label: "OPM",
    aliases: ["opm", "margin", "operating margin"],
    example: "opm > 15",
    value: (row) => row?.value_metrics?.opm_percent ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "dividend_yield",
    label: "Dividend Yield",
    aliases: ["dividend", "yield", "dividend yield"],
    example: "dividend > 2",
    value: (row) => row?.value_metrics?.dividend_yield ?? null,
    type: "number",
    unit: "%",
  },
  {
    key: "pe_ratio",
    label: "P/E",
    aliases: ["pe", "p/e", "price earnings", "price to earnings"],
    example: "pe < 20",
    value: (row) => row?.value_metrics?.pe_ratio ?? null,
    type: "number",
  },
  {
    key: "price_to_book",
    label: "Price to Book",
    aliases: ["pb", "p/b", "price to book", "book value"],
    example: "price to book < 2",
    value: (row) => row?.value_metrics?.price_to_book ?? null,
    type: "number",
  },
  {
    key: "pe_vs_industry",
    label: "P/E vs Industry",
    aliases: ["industry pe", "pe vs industry", "relative pe"],
    example: "pe vs industry < 0.7",
    value: (row) => row?.value_metrics?.pe_vs_industry ?? null,
    type: "number",
  },
  {
    key: "ev_ebitda",
    label: "EV / EBITDA",
    aliases: ["ev", "ebitda", "ev/ebitda"],
    example: "ev / ebitda < 10",
    value: (row) => row?.value_metrics?.ev_ebitda ?? null,
    type: "number",
  },
  {
    key: "interest_coverage",
    label: "Interest Coverage",
    aliases: ["interest coverage", "coverage"],
    example: "interest coverage > 5",
    value: (row) => row?.value_metrics?.interest_coverage ?? null,
    type: "number",
  },
  {
    key: "debtor_days",
    label: "Debtor Days",
    aliases: ["debtor", "receivable days", "debtor days"],
    example: "debtor days < 45",
    value: (row) => row?.value_metrics?.debtor_days ?? null,
    type: "number",
    unit: "days",
  },
  {
    key: "price_to_sales",
    label: "Price to Sales",
    aliases: ["ps", "p/s", "price to sales"],
    example: "price to sales < 1.5",
    value: (row) => row?.value_metrics?.price_to_sales ?? null,
    type: "number",
  },
  {
    key: "market_cap",
    label: "Market Cap",
    aliases: ["market cap", "mcap"],
    example: "market cap > 1000",
    value: (row) => row?.market_cap ?? null,
    type: "number",
  },
];

const normalize = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const toNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const compare = (value, operator, expected) => {
  const left = toNumber(value);
  const right = toNumber(expected);
  if (left === null || right === null) return false;
  switch (operator) {
    case ">":
      return left > right;
    case ">=":
      return left >= right;
    case "<":
      return left < right;
    case "<=":
      return left <= right;
    case "=":
    case "==":
      return left === right;
    case "!=":
      return left !== right;
    default:
      return false;
  }
};

const FIELD_PATTERN = /^(.+?)\s*(>=|<=|!=|==|=|>|<)\s*(.+)$/i;

const resolveFieldCandidates = (queryText = "") => {
  const term = normalize(queryText);
  if (!term) return [];
  return SEARCH_FIELDS.map((field) => {
    const haystack = normalize([field.label, field.key, ...(field.aliases || [])].join(" "));
    let score = 0;
    if (haystack === term) score = 100;
    else if (haystack.startsWith(term)) score = 80;
    else if (haystack.includes(term)) score = 50;
    else {
      const tokens = term.split(" ").filter(Boolean);
      score = tokens.reduce((acc, token) => (haystack.includes(token) ? acc + 10 : acc), 0);
    }
    return { ...field, score };
  })
    .filter((field) => field.score > 0)
    .sort((a, b) => b.score - a.score);
};

const parseQuery = (query = "") => {
  const text = String(query || "").trim();
  if (!text) return [];

  return text
    .split(/\s+(?:and|&&)\s+|[,;]+/i)
    .map((clause) => clause.trim())
    .filter(Boolean)
    .map((clause) => {
      const match = clause.match(FIELD_PATTERN);
      if (!match) {
        return { raw: clause, fieldText: clause, operator: null, valueText: null };
      }
      return {
        raw: clause,
        fieldText: match[1].trim(),
        operator: match[2].trim(),
        valueText: match[3].trim(),
      };
    });
};

const formatValue = (field, value) => {
  if (value === null || value === undefined) return "?";
  if (field.type === "text") return String(value);
  if (field.unit === "%") return `${Number(value).toFixed(2)}%`;
  if (field.unit === "years") return `${Number(value).toFixed(1)} yrs`;
  if (field.unit === "days") return `${Number(value).toFixed(0)} days`;
  if (field.unit === "pp") return `${Number(value).toFixed(2)} pp`;
  return Number.isFinite(Number(value)) ? Number(value).toFixed(2) : String(value);
};

const getSearchUniverseRows = async (db = pool) => {
  const rows = await buildValueAnalysisRows({ tier1Only: false }, db);
  return Array.isArray(rows) ? rows.filter(Boolean) : [];
};

const suggestSearchFields = (query = "") => {
  const matches = resolveFieldCandidates(query);
  return matches.slice(0, 8).map((field) => ({
    key: field.key,
    label: field.label,
    aliases: field.aliases,
    example: field.example,
    type: field.type,
    unit: field.unit || null,
  }));
};

const searchStocks = async ({ query = "", limit = 50 } = {}, db = pool) => {
  const normalizedQuery = String(query || "").trim();
  const clauses = parseQuery(normalizedQuery);
  const rows = await getSearchUniverseRows(db);

  const suggestionOnly = !clauses.length || clauses.every((clause) => !clause.operator);
  if (suggestionOnly && normalizedQuery) {
    return {
      query: normalizedQuery,
      total: 0,
      rows: [],
      parsed: clauses,
      suggestions: suggestSearchFields(normalizedQuery),
    };
  }

  const evaluated = rows
    .map((row) => {
      const matches = [];
      let allPassed = true;

      clauses.forEach((clause) => {
        const candidate = resolveFieldCandidates(clause.fieldText)[0] || null;
        if (!candidate || !clause.operator) {
          allPassed = false;
          matches.push({
            field: clause.fieldText,
            operator: clause.operator,
            threshold: clause.valueText,
            status: "unmatched",
            reason: "Unknown field",
          });
          return;
        }

        const actual = candidate.value(row);
        const passed = compare(actual, clause.operator, clause.valueText);
        if (!passed) allPassed = false;
        matches.push({
          field: candidate.label,
          key: candidate.key,
          operator: clause.operator,
          threshold: clause.valueText,
          actual,
          status: passed ? "pass" : "fail",
          formattedActual: formatValue(candidate, actual),
        });
      });

      return {
        ...row,
        search: {
          query: normalizedQuery,
          clauses,
          matches,
          matched: allPassed,
          matched_count: matches.filter((item) => item.status === "pass").length,
        },
      };
    })
    .filter((row) => row.search.matched)
    .sort((a, b) => {
      const scoreDiff = Number(b?.analysis?.score || 0) - Number(a?.analysis?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const matchDiff = Number(b?.search?.matched_count || 0) - Number(a?.search?.matched_count || 0);
      if (matchDiff !== 0) return matchDiff;
      return String(a?.symbol || "").localeCompare(String(b?.symbol || ""));
    });

  return {
    query: normalizedQuery,
    total: evaluated.length,
    rows: evaluated.slice(0, Number(limit) || 50),
    parsed: clauses,
    suggestions: suggestSearchFields(normalizedQuery),
  };
};

module.exports = {
  SEARCH_FIELDS,
  suggestSearchFields,
  searchStocks,
};
