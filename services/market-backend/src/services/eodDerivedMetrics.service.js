const toTradeDateKey = (value) => {
  if (!value) return null;
  const asString = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(asString)) return asString.slice(0, 10);
  const date = value instanceof Date ? value : new Date(asString);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return year && month && day ? `${year}-${month}-${day}` : null;
};

const INDICATOR_PARAMS = Object.freeze({
  returns: { week: 5, month: 21, threeMonth: 63, sixMonth: 126, year: 252 },
  week52Lookback: 252,
  volume: { avg20: 20, avg50: 50 },
  volatility: { short: 20 },
  atrPeriod: 14,
  rsiPeriod: 14,
  adxPeriod: 14,
  supertrend: { atrPeriod: 10, multiplier: 3 },
  liquidity: { tradedDays20dMin: 18, avgTradedValue20dMin: 1000000 },
});

const roundNumber = (value, decimals = 6) => {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
};

const safeDivide = (num, den) => {
  if (!Number.isFinite(num) || !Number.isFinite(den) || den === 0) return null;
  return num / den;
};

const pctChange = (current, base) => {
  const ratio = safeDivide(current - base, base);
  return ratio === null ? null : ratio * 100;
};

const stdDev = (sum, sumSq, count) => {
  if (!Number.isFinite(sum) || !Number.isFinite(sumSq) || !Number.isFinite(count) || count <= 1) {
    return null;
  }
  const mean = sum / count;
  const variance = Math.max((sumSq / count) - mean * mean, 0);
  return Math.sqrt(variance);
};

const buildMissingHistoryReasons = (rowCount, metrics) => {
  const missing = [];
  if (metrics.dma_20 === null) missing.push("dma_20");
  if (metrics.dma_50 === null) missing.push("dma_50");
  if (metrics.dma_200 === null) missing.push("dma_200");
  if (metrics.return_1y === null) missing.push("return_1y");
  if (metrics.week_52_high === null) missing.push("week_52");
  if (metrics.atr_14 === null) missing.push("atr_14");
  if (rowCount < 1) missing.push("no_history");
  return missing;
};

function computeDerivedMetricsForCandles(candles = [], options = {}) {
  if (!Array.isArray(candles) || !candles.length) return [];

  const n = candles.length;
  const closes = new Array(n);
  const highs = new Array(n);
  const lows = new Array(n);
  const volumes = new Array(n);
  const tradedValues = new Array(n);
  const dailyReturns = new Array(n).fill(null);
  const trueRanges = new Array(n).fill(null);

  for (let i = 0; i < n; i += 1) {
    const candle = candles[i] || {};
    closes[i] = Number(candle.close || 0);
    highs[i] = Number(candle.high || 0);
    lows[i] = Number(candle.low || 0);
    volumes[i] = Number(candle.volume || 0);
    tradedValues[i] = closes[i] * volumes[i];

    if (i > 0) {
      dailyReturns[i] = pctChange(closes[i], closes[i - 1]);
      const range1 = highs[i] - lows[i];
      const range2 = Math.abs(highs[i] - closes[i - 1]);
      const range3 = Math.abs(lows[i] - closes[i - 1]);
      trueRanges[i] = Math.max(range1, range2, range3);
    } else {
      trueRanges[i] = highs[i] - lows[i];
    }
  }

  const metricsByRow = new Array(n);

  let close20Sum = 0;
  let close50Sum = 0;
  let close200Sum = 0;
  let volume20Sum = 0;
  let tradedValue20Sum = 0;
  let tradedDays20Sum = 0;

  let vol20Sum = 0;
  let vol20Sq = 0;
  let vol20Count = 0;

  const prior252HighDeque = [];
  const prior252LowDeque = [];
  const dma20Arr = new Array(n).fill(null);
  const dma50Arr = new Array(n).fill(null);
  const dma200Arr = new Array(n).fill(null);
  const atr14Arr = new Array(n).fill(null);
  const atr10Arr = new Array(n).fill(null);
  const rsi14Arr = new Array(n).fill(null);
  const ema12Arr = new Array(n).fill(null);
  const ema26Arr = new Array(n).fill(null);
  const macdLineArr = new Array(n).fill(null);
  const macdSignalArr = new Array(n).fill(null);
  const adx14Arr = new Array(n).fill(null);
  const supertrendSignalArr = new Array(n).fill(null);

  let allTimePriorHigh = null;

  let atr14Prev = null;
  let sumTr14 = 0;
  let atr10Prev = null;
  let sumTr10 = 0;
  let rsiGainSum = 0;
  let rsiLossSum = 0;
  let avgGain14 = null;
  let avgLoss14 = null;
  let ema12SeedSum = 0;
  let ema26SeedSum = 0;
  let ema12Prev = null;
  let ema26Prev = null;
  let macdSignalSeedSum = 0;
  let macdSignalSeedCount = 0;
  let tr14Sum = 0;
  let plusDm14Sum = 0;
  let minusDm14Sum = 0;
  let tr14Prev = null;
  let plusDm14Prev = null;
  let minusDm14Prev = null;
  let adxDxSeedSum = 0;
  let adxDxSeedCount = 0;
  let adxPrev = null;

  let finalUpperBandPrev = null;
  let finalLowerBandPrev = null;
  let supertrendPrev = null;

  for (let i = 0; i < n; i += 1) {
    const close = closes[i];
    const high = highs[i];
    const low = lows[i];
    const volume = volumes[i];
    const tradedValue = tradedValues[i];

    close20Sum += close;
    close50Sum += close;
    close200Sum += close;
    volume20Sum += volume;
    tradedValue20Sum += tradedValue;
    tradedDays20Sum += volume > 0 ? 1 : 0;

    if (i >= 20) {
      close20Sum -= closes[i - 20];
      volume20Sum -= volumes[i - 20];
      tradedValue20Sum -= tradedValues[i - 20];
      tradedDays20Sum -= volumes[i - 20] > 0 ? 1 : 0;
    }
    if (i >= 50) {
      close50Sum -= closes[i - 50];
    }
    if (i >= 200) {
      close200Sum -= closes[i - 200];
    }

    dma20Arr[i] = i >= 19 ? roundNumber(close20Sum / 20) : null;
    dma50Arr[i] = i >= 49 ? roundNumber(close50Sum / 50) : null;
    dma200Arr[i] = i >= 199 ? roundNumber(close200Sum / 200) : null;

    if (i > 0 && dailyReturns[i] !== null) {
      vol20Sum += dailyReturns[i];
      vol20Sq += dailyReturns[i] * dailyReturns[i];
      vol20Count += 1;
    }
    if (i > 20 && dailyReturns[i - 20] !== null) {
      vol20Sum -= dailyReturns[i - 20];
      vol20Sq -= dailyReturns[i - 20] * dailyReturns[i - 20];
      vol20Count -= 1;
    }

    if (i > 0) {
      const change = close - closes[i - 1];
      const gain = Math.max(change, 0);
      const loss = Math.max(-change, 0);

      if (i <= 14) {
        rsiGainSum += gain;
        rsiLossSum += loss;
        if (i === 14) {
          avgGain14 = rsiGainSum / 14;
          avgLoss14 = rsiLossSum / 14;
          const rs = avgLoss14 === 0 ? Infinity : avgGain14 / avgLoss14;
          rsi14Arr[i] = roundNumber(100 - (100 / (1 + rs)));
        }
      } else if (avgGain14 !== null && avgLoss14 !== null) {
        avgGain14 = ((avgGain14 * 13) + gain) / 14;
        avgLoss14 = ((avgLoss14 * 13) + loss) / 14;
        const rs = avgLoss14 === 0 ? Infinity : avgGain14 / avgLoss14;
        rsi14Arr[i] = roundNumber(100 - (100 / (1 + rs)));
      }
    }

    ema12SeedSum += close;
    ema26SeedSum += close;
    if (i === 11) {
      ema12Prev = ema12SeedSum / 12;
      ema12Arr[i] = roundNumber(ema12Prev);
    } else if (i > 11 && ema12Prev !== null) {
      const k12 = 2 / (12 + 1);
      ema12Prev = ((close - ema12Prev) * k12) + ema12Prev;
      ema12Arr[i] = roundNumber(ema12Prev);
    }
    if (i === 25) {
      ema26Prev = ema26SeedSum / 26;
      ema26Arr[i] = roundNumber(ema26Prev);
    } else if (i > 25 && ema26Prev !== null) {
      const k26 = 2 / (26 + 1);
      ema26Prev = ((close - ema26Prev) * k26) + ema26Prev;
      ema26Arr[i] = roundNumber(ema26Prev);
    }
    if (ema12Prev !== null && ema26Prev !== null) {
      const macdLine = ema12Prev - ema26Prev;
      macdLineArr[i] = roundNumber(macdLine);
      if (macdSignalSeedCount < 9) {
        macdSignalSeedSum += macdLine;
        macdSignalSeedCount += 1;
        if (macdSignalSeedCount === 9) {
          macdSignalArr[i] = roundNumber(macdSignalSeedSum / 9);
        }
      } else if (macdSignalArr[i - 1] !== null) {
        const prevSignal = macdSignalArr[i - 1];
        const nextSignal = ((prevSignal * 8) + macdLine) / 9;
        macdSignalArr[i] = roundNumber(nextSignal);
      }
    }

    if (i > 0) {
      const upMove = high - highs[i - 1];
      const downMove = lows[i - 1] - low;
      const plusDM = upMove > downMove && upMove > 0 ? upMove : 0;
      const minusDM = downMove > upMove && downMove > 0 ? downMove : 0;
      tr14Sum += trueRanges[i];
      plusDm14Sum += plusDM;
      minusDm14Sum += minusDM;
      if (i > 14) {
        tr14Sum -= trueRanges[i - 14];
        plusDm14Sum -= (highs[i - 14] - highs[i - 15] > lows[i - 15] - lows[i - 14] && highs[i - 14] - highs[i - 15] > 0)
          ? highs[i - 14] - highs[i - 15]
          : 0;
        minusDm14Sum -= (lows[i - 15] - lows[i - 14] > highs[i - 14] - highs[i - 15] && lows[i - 15] - lows[i - 14] > 0)
          ? lows[i - 15] - lows[i - 14]
          : 0;
      }

      if (i === 14) {
        tr14Prev = tr14Sum;
        plusDm14Prev = plusDm14Sum;
        minusDm14Prev = minusDm14Sum;
      } else if (i > 14 && tr14Prev !== null) {
        tr14Prev = tr14Prev - (tr14Prev / 14) + trueRanges[i];
        plusDm14Prev = plusDm14Prev - (plusDm14Prev / 14) + plusDM;
        minusDm14Prev = minusDm14Prev - (minusDm14Prev / 14) + minusDM;
      }

      if (tr14Prev !== null && tr14Prev > 0 && plusDm14Prev !== null && minusDm14Prev !== null) {
        const plusDI = (plusDm14Prev / tr14Prev) * 100;
        const minusDI = (minusDm14Prev / tr14Prev) * 100;
        const dx = plusDI + minusDI === 0 ? null : (Math.abs(plusDI - minusDI) / (plusDI + minusDI)) * 100;
        if (dx !== null) {
          if (adxDxSeedCount < 14) {
            adxDxSeedSum += dx;
            adxDxSeedCount += 1;
            if (adxDxSeedCount === 14) {
              adxPrev = adxDxSeedSum / 14;
              adx14Arr[i] = roundNumber(adxPrev);
            }
          } else if (adxPrev !== null) {
            adxPrev = ((adxPrev * 13) + dx) / 14;
            adx14Arr[i] = roundNumber(adxPrev);
          }
        }
      }
    }

    while (prior252HighDeque.length && prior252HighDeque[0] <= i - 253) prior252HighDeque.shift();
    while (prior252LowDeque.length && prior252LowDeque[0] <= i - 253) prior252LowDeque.shift();

    const prior252High = i >= 252 && prior252HighDeque.length ? highs[prior252HighDeque[0]] : null;
    const prior252Low = i >= 252 && prior252LowDeque.length ? lows[prior252LowDeque[0]] : null;
    const priorAth = allTimePriorHigh;

    const trueRange = trueRanges[i];
    sumTr14 += trueRange;
    if (i >= 14) sumTr14 -= trueRanges[i - 14];

    if (i === 13) {
      atr14Prev = sumTr14 / 14;
      atr14Arr[i] = roundNumber(atr14Prev);
    } else if (i > 13 && atr14Prev !== null) {
      atr14Prev = ((atr14Prev * 13) + trueRange) / 14;
      atr14Arr[i] = roundNumber(atr14Prev);
    }

    sumTr10 += trueRange;
    if (i >= 10) sumTr10 -= trueRanges[i - 10];
    if (i === 9) {
      atr10Prev = sumTr10 / 10;
      atr10Arr[i] = roundNumber(atr10Prev);
    } else if (i > 9 && atr10Prev !== null) {
      atr10Prev = ((atr10Prev * 9) + trueRange) / 10;
      atr10Arr[i] = roundNumber(atr10Prev);
    }

    const metrics = {
      dma_20: dma20Arr[i],
      dma_50: dma50Arr[i],
      dma_200: dma200Arr[i],
      return_1m: i >= 21 ? roundNumber(pctChange(close, closes[i - 21])) : null,
      return_3m: i >= 63 ? roundNumber(pctChange(close, closes[i - 63])) : null,
      return_6m: i >= 126 ? roundNumber(pctChange(close, closes[i - 126])) : null,
      return_1y: i >= 252 ? roundNumber(pctChange(close, closes[i - 252])) : null,
      week_52_high: prior252High === null ? null : roundNumber(prior252High),
      week_52_low: prior252Low === null ? null : roundNumber(prior252Low),
      week_52_high_breakout:
        prior252High === null ? null : close >= prior252High,
      all_time_high: priorAth === null ? null : roundNumber(priorAth),
      all_time_high_breakout:
        priorAth === null ? null : close >= priorAth,
      avg_volume_20d: i >= 19 ? Math.round(volume20Sum / 20) : null,
      avg_traded_value_20d: i >= 19 ? Math.round(tradedValue20Sum / 20) : null,
      traded_days_20d: i >= 19 ? tradedDays20Sum : null,
      volatility_20d:
        i >= 20 && vol20Count === 20 ? roundNumber(stdDev(vol20Sum, vol20Sq, vol20Count) * 100) : null,
      atr_14: i >= 14 ? atr14Arr[i] : null,
      rsi_14: rsi14Arr[i],
      macd_line: macdLineArr[i],
      macd_signal: macdSignalArr[i],
      adx_14: adx14Arr[i],
      supertrend_signal: supertrendSignalArr[i],
      is_liquid:
        i >= 19
          ? tradedDays20Sum >= INDICATOR_PARAMS.liquidity.tradedDays20dMin
            && (tradedValue20Sum / 20) >= INDICATOR_PARAMS.liquidity.avgTradedValue20dMin
          : null,
    };

    if (atr10Arr[i] !== null) {
      const hl2 = (high + low) / 2;
      const basicUpperBand = hl2 + (INDICATOR_PARAMS.supertrend.multiplier * atr10Arr[i]);
      const basicLowerBand = hl2 - (INDICATOR_PARAMS.supertrend.multiplier * atr10Arr[i]);

      const finalUpperBand =
        finalUpperBandPrev === null || basicUpperBand < finalUpperBandPrev || closes[i - 1] > finalUpperBandPrev
          ? basicUpperBand
          : finalUpperBandPrev;
      const finalLowerBand =
        finalLowerBandPrev === null || basicLowerBand > finalLowerBandPrev || closes[i - 1] < finalLowerBandPrev
          ? basicLowerBand
          : finalLowerBandPrev;

      let supertrend = null;
      if (supertrendPrev === null) {
        supertrend = close <= finalUpperBand ? finalUpperBand : finalLowerBand;
      } else if (supertrendPrev === finalUpperBandPrev) {
        supertrend = close <= finalUpperBand ? finalUpperBand : finalLowerBand;
      } else {
        supertrend = close >= finalLowerBand ? finalLowerBand : finalUpperBand;
      }

      const signal = close > supertrend ? 1 : -1;
      supertrendSignalArr[i] = signal;
      finalUpperBandPrev = finalUpperBand;
      finalLowerBandPrev = finalLowerBand;
      supertrendPrev = supertrend;
    }

    metrics.supertrend_signal = supertrendSignalArr[i];
    metricsByRow[i] = metrics;

    while (prior252HighDeque.length && highs[prior252HighDeque[prior252HighDeque.length - 1]] <= high) {
      prior252HighDeque.pop();
    }
    prior252HighDeque.push(i);

    while (prior252LowDeque.length && lows[prior252LowDeque[prior252LowDeque.length - 1]] >= low) {
      prior252LowDeque.pop();
    }
    prior252LowDeque.push(i);

    allTimePriorHigh = allTimePriorHigh === null ? high : Math.max(allTimePriorHigh, high);
  }

  return candles.map((candle, index) => ({
    master_id: Number(candle.master_id),
    trade_date: toTradeDateKey(candle.trade_date),
    ...metricsByRow[index],
  }));
}

module.exports = {
  INDICATOR_PARAMS,
  toTradeDateKey,
  computeDerivedMetricsForCandles,
};
