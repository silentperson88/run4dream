const { pool } = require("../config/db");

const getMomentumSnapshotByMasterId = async (masterId, db = pool) => {
  const { rows } = await db.query(
    `
      WITH base AS (
        SELECT
          master_id,
          symbol,
          exchange,
          trade_date,
          open,
          high,
          low,
          close,
          volume
        FROM eod
        WHERE master_id = $1
      ),
      latest_base AS (
        SELECT *
        FROM base
        ORDER BY trade_date DESC
        LIMIT 1
      ),
      price_changes AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          close,
          close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_change
        FROM base
      ),
      gains_losses AS (
        SELECT
          *,
          CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
          CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
        FROM price_changes
        WHERE price_change IS NOT NULL
      ),
      avg_gains_losses AS (
        SELECT
          *,
          AVG(gain) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING
          ) AS avg_gain_14,
          AVG(loss) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING
          ) AS avg_loss_14
        FROM gains_losses
      ),
      rsi_result AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          ROUND(rsi_value::NUMERIC, 2) AS rsi_14,
          CASE
            WHEN rsi_value IS NULL THEN NULL
            WHEN rsi_value >= 70 THEN 'Overbought'
            WHEN rsi_value <= 30 THEN 'Oversold'
            WHEN rsi_value >= 50 THEN 'Bullish'
            ELSE 'Bearish'
          END AS rsi_signal
        FROM (
          SELECT
            *,
            CASE
              WHEN avg_gain_14 IS NULL OR avg_loss_14 IS NULL THEN NULL
              WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50
              WHEN avg_loss_14 = 0 THEN 100
              ELSE 100 - (100 / (1 + avg_gain_14 / avg_loss_14))
            END AS rsi_value
          FROM avg_gains_losses
        ) rsi_base
      ),
      ema_calc AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          close,
          AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING
          ) AS ema_12,
          AVG(close) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING
          ) AS ema_26
        FROM base
      ),
      macd_calc AS (
        SELECT
          *,
          ROUND((ema_12 - ema_26)::NUMERIC, 2) AS macd_line
        FROM ema_calc
        WHERE ema_26 IS NOT NULL
      ),
      macd_signal AS (
        SELECT
          *,
          ROUND(
            AVG(macd_line) OVER (
              PARTITION BY symbol ORDER BY trade_date ROWS 8 PRECEDING
            )::NUMERIC,
            2
          ) AS signal_line
        FROM macd_calc
      ),
      macd_result AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          macd_line,
          signal_line,
          ROUND((macd_line - signal_line)::NUMERIC, 2) AS macd_histogram,
          CASE
            WHEN macd_line IS NULL OR signal_line IS NULL THEN NULL
            WHEN macd_line > signal_line AND
                 LAG(macd_line) OVER (PARTITION BY symbol ORDER BY trade_date) <=
                 LAG(signal_line) OVER (PARTITION BY symbol ORDER BY trade_date)
              THEN 'Bullish Crossover'
            WHEN macd_line < signal_line AND
                 LAG(macd_line) OVER (PARTITION BY symbol ORDER BY trade_date) >=
                 LAG(signal_line) OVER (PARTITION BY symbol ORDER BY trade_date)
              THEN 'Bearish Crossover'
            WHEN macd_line > signal_line THEN 'Bullish'
            ELSE 'Bearish'
          END AS macd_signal
        FROM macd_signal
      ),
      roc_result AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          ROUND(
            ((close - LAG(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date))
              / NULLIF(LAG(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
            )::NUMERIC,
            2
          ) AS roc_10d,
          ROUND(
            ((close - LAG(close, 20) OVER (PARTITION BY symbol ORDER BY trade_date))
              / NULLIF(LAG(close, 20) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
            )::NUMERIC,
            2
          ) AS roc_20d,
          ROUND(
            ((close - LAG(close, 60) OVER (PARTITION BY symbol ORDER BY trade_date))
              / NULLIF(LAG(close, 60) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
            )::NUMERIC,
            2
          ) AS roc_60d,
          ROUND(
            ((close - LAG(close, 252) OVER (PARTITION BY symbol ORDER BY trade_date))
              / NULLIF(LAG(close, 252) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100
            )::NUMERIC,
            2
          ) AS roc_1yr
        FROM base
      ),
      stoch_calc AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          close,
          MIN(low) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING
          ) AS lowest_14,
          MAX(high) OVER (
            PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING
          ) AS highest_14
        FROM base
      ),
      stoch_k AS (
        SELECT
          *,
          ROUND(
            ((close - lowest_14) / NULLIF(highest_14 - lowest_14, 0) * 100)::NUMERIC,
            2
          ) AS stoch_k
        FROM stoch_calc
      ),
      stoch_result AS (
        SELECT
          symbol,
          exchange,
          trade_date,
          stoch_k,
          ROUND(
            AVG(stoch_k) OVER (
              PARTITION BY symbol ORDER BY trade_date ROWS 2 PRECEDING
            )::NUMERIC,
            2
          ) AS stoch_d,
          CASE
            WHEN stoch_k >= 80 THEN 'Overbought'
            WHEN stoch_k <= 20 THEN 'Oversold'
            ELSE 'Neutral'
          END AS stoch_signal
        FROM stoch_k
        WHERE stoch_k IS NOT NULL
      )
      SELECT
        lb.master_id,
        lb.symbol,
        lb.exchange,
        lb.trade_date,
        lb.open,
        lb.high,
        lb.low,
        lb.close,
        lb.volume,
        r.rsi_14,
        r.rsi_signal,
        m.macd_line,
        m.signal_line,
        m.macd_histogram,
        m.macd_signal,
        ro.roc_10d,
        ro.roc_20d,
        ro.roc_60d,
        ro.roc_1yr,
        s.stoch_k,
        s.stoch_d,
        s.stoch_signal,
        CASE
          WHEN r.rsi_14 > 60 AND COALESCE(m.macd_line, 0) > 0 AND COALESCE(ro.roc_20d, 0) > 0 THEN 'Strong Bullish'
          WHEN r.rsi_14 > 50 AND COALESCE(m.macd_line, 0) > 0 THEN 'Bullish'
          WHEN r.rsi_14 < 40 AND COALESCE(m.macd_line, 0) < 0 AND COALESCE(ro.roc_20d, 0) < 0 THEN 'Strong Bearish'
          WHEN r.rsi_14 < 50 AND COALESCE(m.macd_line, 0) < 0 THEN 'Bearish'
          ELSE 'Neutral'
        END AS momentum_score
      FROM latest_base lb
      LEFT JOIN rsi_result r
        ON r.symbol = lb.symbol AND r.trade_date = lb.trade_date
      LEFT JOIN macd_result m
        ON m.symbol = lb.symbol AND m.trade_date = lb.trade_date
      LEFT JOIN roc_result ro
        ON ro.symbol = lb.symbol AND ro.trade_date = lb.trade_date
      LEFT JOIN stoch_result s
        ON s.symbol = lb.symbol AND s.trade_date = lb.trade_date
      LIMIT 1
    `,
    [Number(masterId)],
  );

  return rows[0] || null;
};

module.exports = {
  getMomentumSnapshotByMasterId,
};
