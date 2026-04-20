const normalizeAsOfDate = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return text;
};

const getAsOfDateFromRequest = (req) => normalizeAsOfDate(req?.query?.as_of_date || req?.headers?.["x-as-of-date"]);

const asOfDateToPeriodNumericValue = (asOfDate) => {
  const normalized = normalizeAsOfDate(asOfDate);
  if (!normalized) return null;
  const [year, month] = normalized.split("-");
  return Number(year) * 100 + Number(month);
};

const periodNumericToValue = (periodNumeric) => {
  const text = String(periodNumeric || "").trim();
  const match = text.match(/^(\d{2})-(\d{4})$/);
  if (!match) return null;
  return Number(match[2]) * 100 + Number(match[1]);
};

const filterRowsByAsOfDate = (rows = [], asOfDate) => {
  const cutoff = asOfDateToPeriodNumericValue(asOfDate);
  if (!cutoff) return rows;
  return rows.filter((row) => {
    const value = periodNumericToValue(row?.period_numeric);
    if (value === null) return true;
    return value <= cutoff;
  });
};

module.exports = {
  normalizeAsOfDate,
  getAsOfDateFromRequest,
  asOfDateToPeriodNumericValue,
  periodNumericToValue,
  filterRowsByAsOfDate,
};
