const { pool } = require("../config/db");
const { normalizeAsOfDate } = require("../utils/asOfDate.utils");

const getIndiaTodayIso = () => {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return `${year}-${month}-${day}`;
};

const toNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const round = (value, digits = 2) => {
  const num = toNumber(value);
  if (num === null) return null;
  const factor = 10 ** digits;
  return Math.round(num * factor) / factor;
};

const formatPercent = (value) => {
  const num = round(value, 2);
  if (num === null) return null;
  return `${num > 0 ? "+" : ""}${num}%`;
};

async function getUpperLowerCircuitAlerts(asOfDate, db = pool) {
  const { rows } = await db.query(
    `
      WITH target_rows AS (
        SELECT
          e.master_id,
          sm.symbol,
          sm.name,
          e.trade_date,
          e.ltp,
          e.close,
          e.volume,
          e.upper_circuit,
          e.lower_circuit
        FROM eod e
        INNER JOIN stock_master sm ON sm.id = e.master_id
        WHERE e.trade_date = $1::date
      )
      SELECT
        t.*,
        CASE
          WHEN t.upper_circuit IS NOT NULL AND COALESCE(t.ltp, t.close) >= t.upper_circuit THEN 'UPPER_CIRCUIT'
          WHEN t.lower_circuit IS NOT NULL AND COALESCE(t.ltp, t.close) <= t.lower_circuit THEN 'LOWER_CIRCUIT'
          ELSE NULL
        END AS alert_type,
        CASE
          WHEN p.prev_close IS NOT NULL AND p.prev_close <> 0
            THEN ((COALESCE(t.ltp, t.close) - p.prev_close) / p.prev_close) * 100
          ELSE NULL
        END AS day_move_percent
      FROM target_rows t
      LEFT JOIN LATERAL (
        SELECT e_prev.close AS prev_close
        FROM eod e_prev
        WHERE e_prev.master_id = t.master_id
          AND e_prev.trade_date < t.trade_date
        ORDER BY e_prev.trade_date DESC
        LIMIT 1
      ) p ON TRUE
        WHERE (
          (t.upper_circuit IS NOT NULL AND COALESCE(t.ltp, t.close) >= t.upper_circuit)
          OR (t.lower_circuit IS NOT NULL AND COALESCE(t.ltp, t.close) <= t.lower_circuit)
        )
      ORDER BY ABS(COALESCE(
        CASE
          WHEN p.prev_close IS NOT NULL AND p.prev_close <> 0
            THEN ((COALESCE(t.ltp, t.close) - p.prev_close) / p.prev_close) * 100
          ELSE 0
        END,
        0
      )) DESC, t.volume DESC, t.symbol ASC
    `,
    [asOfDate],
  );

  return rows.map((row) => {
    const isUpper = row.alert_type === "UPPER_CIRCUIT";
    const move = formatPercent(row.day_move_percent);
    const limit = round(isUpper ? row.upper_circuit : row.lower_circuit, 2);
    return {
      master_id: Number(row.master_id),
      symbol: row.symbol,
      stock: row.name || row.symbol,
      headline: isUpper
        ? `${row.symbol} hit upper circuit today${move ? ` with ${move}` : ""}`
        : `${row.symbol} hit lower circuit today${move ? ` with ${move}` : ""}`,
      angle: isUpper
        ? `Closed at the day limit near Rs ${limit ?? "-"}, which makes it a strong short-format momentum alert.`
        : `Closed at the lower limit near Rs ${limit ?? "-"}, which is useful for panic-sell reaction content.`,
      metrics: {
        trade_date: row.trade_date,
        ltp: round(row.ltp, 2),
        close: round(row.close, 2),
        day_move_percent: round(row.day_move_percent, 2),
        upper_circuit: round(row.upper_circuit, 2),
        lower_circuit: round(row.lower_circuit, 2),
        alert_type: row.alert_type,
      },
    };
  });
}

async function getWeek52Breakouts(asOfDate, db = pool) {
  const { rows } = await db.query(
    `
      WITH target_rows AS (
        SELECT
          e.master_id,
          sm.symbol,
          sm.name,
          e.trade_date,
          e.close,
          e.volume,
          e.week_52_high,
          e.week_52_high_breakout
        FROM eod e
        INNER JOIN stock_master sm ON sm.id = e.master_id
        WHERE e.trade_date = $1::date
      )
      SELECT
        t.*,
        CASE
          WHEN p.prev_close IS NOT NULL AND p.prev_close <> 0
            THEN ((t.close - p.prev_close) / p.prev_close) * 100
          ELSE NULL
        END AS day_move_percent
      FROM target_rows t
      LEFT JOIN LATERAL (
        SELECT e_prev.close AS prev_close
        FROM eod e_prev
        WHERE e_prev.master_id = t.master_id
          AND e_prev.trade_date < t.trade_date
        ORDER BY e_prev.trade_date DESC
        LIMIT 1
      ) p ON TRUE
      WHERE t.week_52_high_breakout = TRUE
      ORDER BY COALESCE(
        CASE
          WHEN p.prev_close IS NOT NULL AND p.prev_close <> 0
            THEN ((t.close - p.prev_close) / p.prev_close) * 100
          ELSE 0
        END,
        0
      ) DESC, t.volume DESC, t.symbol ASC
    `,
    [asOfDate],
  );

  return rows.map((row) => ({
    master_id: Number(row.master_id),
    symbol: row.symbol,
    stock: row.name || row.symbol,
    headline: `${row.symbol} is making a fresh 52-week breakout today`,
    angle: `Trading near Rs ${round(row.close, 2) ?? "-"} and clearing the previous 52-week high around Rs ${round(row.week_52_high, 2) ?? "-"}.`,
    metrics: {
      trade_date: row.trade_date,
      close: round(row.close, 2),
      day_move_percent: round(row.day_move_percent, 2),
      week_52_high: round(row.week_52_high, 2),
      breakout: true,
    },
  }));
}

async function getGoldenDeathCrossSignals(asOfDate, db = pool) {
  const { rows } = await db.query(
    `
      WITH target_rows AS (
        SELECT
          e.master_id,
          sm.symbol,
          sm.name,
          e.trade_date,
          e.close,
          e.dma_50,
          e.dma_200
        FROM eod e
        INNER JOIN stock_master sm ON sm.id = e.master_id
        WHERE e.trade_date = $1::date
      )
      SELECT
        t.*,
        CASE
          WHEN p.prev_dma_50 IS NOT NULL AND p.prev_dma_200 IS NOT NULL
            AND p.prev_dma_50 <= p.prev_dma_200 AND t.dma_50 > t.dma_200 THEN 'GOLDEN_CROSS'
          WHEN p.prev_dma_50 IS NOT NULL AND p.prev_dma_200 IS NOT NULL
            AND p.prev_dma_50 >= p.prev_dma_200 AND t.dma_50 < t.dma_200 THEN 'DEATH_CROSS'
          ELSE NULL
        END AS signal_type
      FROM target_rows t
      LEFT JOIN LATERAL (
        SELECT
          e_prev.dma_50 AS prev_dma_50,
          e_prev.dma_200 AS prev_dma_200
        FROM eod e_prev
        WHERE e_prev.master_id = t.master_id
          AND e_prev.trade_date < t.trade_date
        ORDER BY e_prev.trade_date DESC
        LIMIT 1
      ) p ON TRUE
      WHERE t.dma_50 IS NOT NULL
        AND t.dma_200 IS NOT NULL
        AND (
          (p.prev_dma_50 <= p.prev_dma_200 AND t.dma_50 > t.dma_200)
          OR (p.prev_dma_50 >= p.prev_dma_200 AND t.dma_50 < t.dma_200)
        )
      ORDER BY ABS(t.dma_50 - t.dma_200) DESC, t.symbol ASC
    `,
    [asOfDate],
  );

  return rows.map((row) => {
    const signalType = row.signal_type;
    const bullish = signalType === "GOLDEN_CROSS";
    return {
      master_id: Number(row.master_id),
      symbol: row.symbol,
      stock: row.name || row.symbol,
      headline: bullish
        ? `${row.symbol} just gave a bullish golden cross`
        : `${row.symbol} just gave a bearish death cross`,
      angle: bullish
        ? `50 DMA moved above 200 DMA on ${asOfDate}, which is a classic long-term bullish signal.`
        : `50 DMA slipped below 200 DMA on ${asOfDate}, which is a classic long-term bearish signal.`,
      metrics: {
        trade_date: row.trade_date,
        close: round(row.close, 2),
        dma_50: round(row.dma_50, 2),
        dma_200: round(row.dma_200, 2),
        signal_type: signalType,
      },
    };
  });
}

async function getStockShortsTopics({ asOfDate } = {}, db = pool) {
  const normalizedAsOfDate = normalizeAsOfDate(asOfDate) || getIndiaTodayIso();

  const [circuitAlerts, week52Breakouts, crossoverSignals] = await Promise.all([
    getUpperLowerCircuitAlerts(normalizedAsOfDate, db),
    getWeek52Breakouts(normalizedAsOfDate, db),
    getGoldenDeathCrossSignals(normalizedAsOfDate, db),
  ]);

  return {
    as_of_date: normalizedAsOfDate,
    topics: {
      upper_lower_circuit_alerts: circuitAlerts,
      week_52_breakouts: week52Breakouts,
      golden_death_cross: crossoverSignals,
    },
  };
}

module.exports = {
  getStockShortsTopics,
};
