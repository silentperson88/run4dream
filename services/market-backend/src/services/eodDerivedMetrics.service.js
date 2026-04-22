const CALCULATION_VERSION = "eod-derived-v1";

const toTradeDateKey = (value) => {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const asString = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(asString)) return asString.slice(0, 10);
  const date = new Date(asString);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
};

const INDICATOR_PARAMS = Object.freeze({
  dmaSlopeLookback: 10,
  returns: { week: 5, month: 21, threeMonth: 63, sixMonth: 126, year: 252 },
  week52Lookback: 252,
  dma: { short: 20, mid: 50, long: 200 },
  volume: { avg20: 20, avg50: 50 },
  volatility: { short: 20, long: 50 },
  atrPeriod: 14,
  rsiPeriod: 14,
  adxPeriod: 14,
  macd: { fast: 12, slow: 26, signal: 9 },
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

const pctFromDelta = (delta, base) => {
  const ratio = safeDivide(delta, base);
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
  if (metrics.rsi_14 === null) missing.push("rsi_14");
  if (metrics.macd_line === null) missing.push("macd");
  if (metrics.adx_14 === null) missing.push("adx_14");
  if (metrics.atr_14 === null) missing.push("atr_14");
  if (rowCount < 1) missing.push("no_history");
  return missing;
};

function computeDerivedMetricsForCandles(candles = []) {
  if (!Array.isArray(candles) || !candles.length) return [];

  const n = candles.length;
  const closes = new Array(n);
  const highs = new Array(n);
  const lows = new Array(n);
  const volumes = new Array(n);
  const tradedValues = new Array(n);
  const dailyReturns = new Array(n).fill(null);
  const trueRanges = new Array(n).fill(null);
  const obvValues = new Array(n).fill(0);

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

      if (closes[i] > closes[i - 1]) {
        obvValues[i] = obvValues[i - 1] + volumes[i];
      } else if (closes[i] < closes[i - 1]) {
        obvValues[i] = obvValues[i - 1] - volumes[i];
      } else {
        obvValues[i] = obvValues[i - 1];
      }
    } else {
      trueRanges[i] = highs[i] - lows[i];
      obvValues[i] = volumes[i];
    }
  }

  const metricsByRow = new Array(n);

  let close20Sum = 0;
  let close50Sum = 0;
  let close200Sum = 0;
  let low20Sum = 0;
  let volume20Sum = 0;
  let tradedValue20Sum = 0;
  let tradedValue50Sum = 0;
  let tradedDays20Sum = 0;

  let vol20Sum = 0;
  let vol20Sq = 0;
  let vol20Count = 0;
  let vol50Sum = 0;
  let vol50Sq = 0;
  let vol50Count = 0;

  const prior252HighDeque = [];
  const prior252LowDeque = [];
  const high20Deque = [];

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
  const macdHistArr = new Array(n).fill(null);
  const adx14Arr = new Array(n).fill(null);
  const supertrendArr = new Array(n).fill(null);
  const supertrendSignalArr = new Array(n).fill(null);

  let allTimePriorHigh = null;

  let atr14Prev = null;
  let atr10Prev = null;
  let sumTr14 = 0;
  let sumTr10 = 0;

  let rsiGainSum = 0;
  let rsiLossSum = 0;
  let avgGain14 = null;
  let avgLoss14 = null;

  let ema12Sum = 0;
  let ema26Sum = 0;
  let macdSignalSeedSum = 0;
  let macdSignalSeedCount = 0;

  let adxTrSum = 0;
  let adxPlusDmSum = 0;
  let adxMinusDmSum = 0;
  let adxTr14 = null;
  let adxPlusDm14 = null;
  let adxMinusDm14 = null;
  let adxDxSeedSum = 0;
  let adxDxSeedCount = 0;
  let adxPrev = null;

  let finalUpperBandPrev = null;
  let finalLowerBandPrev = null;
  let supertrendPrev = null;
  let supertrendSignalPrev = null;

  for (let i = 0; i < n; i += 1) {
    const close = closes[i];
    const high = highs[i];
    const low = lows[i];
    const volume = volumes[i];
    const tradedValue = tradedValues[i];

    close20Sum += close;
    close50Sum += close;
    close200Sum += close;
    low20Sum += low;
    volume20Sum += volume;
    tradedValue20Sum += tradedValue;
    tradedValue50Sum += tradedValue;
    tradedDays20Sum += volume > 0 ? 1 : 0;

    if (i >= 20) {
      close20Sum -= closes[i - 20];
      low20Sum -= lows[i - 20];
      volume20Sum -= volumes[i - 20];
      tradedValue20Sum -= tradedValues[i - 20];
      tradedDays20Sum -= volumes[i - 20] > 0 ? 1 : 0;
    }
    if (i >= 50) {
      close50Sum -= closes[i - 50];
      tradedValue50Sum -= tradedValues[i - 50];
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
      vol50Sum += dailyReturns[i];
      vol50Sq += dailyReturns[i] * dailyReturns[i];
      vol50Count += 1;
    }
    if (i > 20 && dailyReturns[i - 20] !== null) {
      vol20Sum -= dailyReturns[i - 20];
      vol20Sq -= dailyReturns[i - 20] * dailyReturns[i - 20];
      vol20Count -= 1;
    }
    if (i > 50 && dailyReturns[i - 50] !== null) {
      vol50Sum -= dailyReturns[i - 50];
      vol50Sq -= dailyReturns[i - 50] * dailyReturns[i - 50];
      vol50Count -= 1;
    }

    while (prior252HighDeque.length && prior252HighDeque[0] <= i - 253) prior252HighDeque.shift();
    while (prior252LowDeque.length && prior252LowDeque[0] <= i - 253) prior252LowDeque.shift();
    while (high20Deque.length && high20Deque[0] <= i - 20) high20Deque.shift();

    const prior252High = i >= 252 && prior252HighDeque.length ? highs[prior252HighDeque[0]] : null;
    const prior252Low = i >= 252 && prior252LowDeque.length ? lows[prior252LowDeque[0]] : null;
    const priorAth = allTimePriorHigh;
    const distanceFrom52wHighPct = prior252High === null ? null : pctFromDelta(prior252High - close, prior252High);
    const distanceFrom52wLowPct = prior252Low === null ? null : pctFromDelta(close - prior252Low, prior252Low);
    const distanceFromAthPct = priorAth === null ? null : pctFromDelta(priorAth - close, priorAth);

    const trueRange = trueRanges[i];
    sumTr14 += trueRange;
    sumTr10 += trueRange;
    if (i >= 14) sumTr14 -= trueRanges[i - 14];
    if (i >= 10) sumTr10 -= trueRanges[i - 10];

    if (i === 13) {
      atr14Prev = sumTr14 / 14;
      atr14Arr[i] = roundNumber(atr14Prev);
    } else if (i > 13 && atr14Prev !== null) {
      atr14Prev = ((atr14Prev * 13) + trueRange) / 14;
      atr14Arr[i] = roundNumber(atr14Prev);
    }

    if (i === 9) {
      atr10Prev = sumTr10 / 10;
    } else if (i > 9 && atr10Prev !== null) {
      atr10Prev = ((atr10Prev * 9) + trueRange) / 10;
    }
    atr10Arr[i] = atr10Prev === null ? null : roundNumber(atr10Prev);

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
        }
      } else if (avgGain14 !== null && avgLoss14 !== null) {
        avgGain14 = ((avgGain14 * 13) + gain) / 14;
        avgLoss14 = ((avgLoss14 * 13) + loss) / 14;
      }
    }

    if (i >= 27 && avgGain14 !== null && avgLoss14 !== null) {
      const rs = avgLoss14 === 0 ? Infinity : avgGain14 / avgLoss14;
      rsi14Arr[i] = roundNumber(100 - (100 / (1 + rs)));
    }

    ema12Sum += close;
    ema26Sum += close;
    if (i === 11) {
      ema12Arr[i] = ema12Sum / 12;
    } else if (i > 11 && ema12Arr[i - 1] !== null) {
      const k12 = 2 / (12 + 1);
      ema12Arr[i] = ((close - ema12Arr[i - 1]) * k12) + ema12Arr[i - 1];
    }

    if (i === 25) {
      ema26Arr[i] = ema26Sum / 26;
    } else if (i > 25 && ema26Arr[i - 1] !== null) {
      const k26 = 2 / (26 + 1);
      ema26Arr[i] = ((close - ema26Arr[i - 1]) * k26) + ema26Arr[i - 1];
    }

    if (ema12Arr[i] !== null && ema26Arr[i] !== null) {
      macdLineArr[i] = ema12Arr[i] - ema26Arr[i];
      if (macdSignalArr[i - 1] === undefined) {
        macdSignalArr[i - 1] = null;
      }
      if (macdSignalSeedCount < 9) {
        macdSignalSeedSum += macdLineArr[i];
        macdSignalSeedCount += 1;
        if (macdSignalSeedCount === 9) {
          macdSignalArr[i] = macdSignalSeedSum / 9;
        }
      } else if (macdSignalArr[i - 1] !== null) {
        const kSignal = 2 / (9 + 1);
        macdSignalArr[i] = ((macdLineArr[i] - macdSignalArr[i - 1]) * kSignal) + macdSignalArr[i - 1];
      }
    }

    if (i >= 34 && macdLineArr[i] !== null && macdSignalArr[i] !== null) {
      macdHistArr[i] = macdLineArr[i] - macdSignalArr[i];
    }

    if (i > 0) {
      const upMove = high - highs[i - 1];
      const downMove = lows[i - 1] - low;
      const plusDm = upMove > downMove && upMove > 0 ? upMove : 0;
      const minusDm = downMove > upMove && downMove > 0 ? downMove : 0;

      adxTrSum += trueRange;
      adxPlusDmSum += plusDm;
      adxMinusDmSum += minusDm;

      if (i > 14) {
        const oldUpMove = highs[i - 14] - highs[i - 15];
        const oldDownMove = lows[i - 15] - lows[i - 14];
        const oldPlusDm = oldUpMove > oldDownMove && oldUpMove > 0 ? oldUpMove : 0;
        const oldMinusDm = oldDownMove > oldUpMove && oldDownMove > 0 ? oldDownMove : 0;
        adxTrSum -= trueRanges[i - 14];
        adxPlusDmSum -= oldPlusDm;
        adxMinusDmSum -= oldMinusDm;
      }

      if (i === 14) {
        adxTr14 = adxTrSum;
        adxPlusDm14 = adxPlusDmSum;
        adxMinusDm14 = adxMinusDmSum;
      } else if (i > 14 && adxTr14 !== null && adxPlusDm14 !== null && adxMinusDm14 !== null) {
        adxTr14 = adxTr14 - (adxTr14 / 14) + trueRange;
        adxPlusDm14 = adxPlusDm14 - (adxPlusDm14 / 14) + plusDm;
        adxMinusDm14 = adxMinusDm14 - (adxMinusDm14 / 14) + minusDm;
      }

      if (adxTr14 !== null && adxTr14 > 0) {
        const plusDi = (adxPlusDm14 / adxTr14) * 100;
        const minusDi = (adxMinusDm14 / adxTr14) * 100;
        const dx = plusDi + minusDi === 0 ? null : (Math.abs(plusDi - minusDi) / (plusDi + minusDi)) * 100;
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
            if (i >= 27) {
              adx14Arr[i] = roundNumber(adxPrev);
            }
          }
        }
      }
    }

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
      supertrendArr[i] = roundNumber(supertrend);
      supertrendSignalArr[i] = signal;
      finalUpperBandPrev = finalUpperBand;
      finalLowerBandPrev = finalLowerBand;
      supertrendPrev = supertrend;
      supertrendSignalPrev = signal;
    } else if (supertrendSignalPrev !== null) {
      supertrendSignalArr[i] = supertrendSignalPrev;
    }

    const metrics = {
      dma_20: dma20Arr[i],
      dma_50: dma50Arr[i],
      dma_200: dma200Arr[i],
      dma_50_slope:
        dma50Arr[i] !== null && i >= 10 && dma50Arr[i - 10] !== null
          ? roundNumber(dma50Arr[i] - dma50Arr[i - 10])
          : null,
      dma_200_slope:
        dma200Arr[i] !== null && i >= 10 && dma200Arr[i - 10] !== null
          ? roundNumber(dma200Arr[i] - dma200Arr[i - 10])
          : null,
      price_vs_dma_50_pct:
        dma50Arr[i] !== null ? roundNumber(pctChange(close, dma50Arr[i])) : null,
      price_vs_dma_200_pct:
        dma200Arr[i] !== null ? roundNumber(pctChange(close, dma200Arr[i])) : null,
      dma_50_vs_dma_200:
        dma50Arr[i] !== null && dma200Arr[i] !== null
          ? roundNumber(pctFromDelta(dma50Arr[i] - dma200Arr[i], dma200Arr[i]))
          : null,
      return_1w: i >= 5 ? roundNumber(pctChange(close, closes[i - 5])) : null,
      return_1m: i >= 21 ? roundNumber(pctChange(close, closes[i - 21])) : null,
      return_3m: i >= 63 ? roundNumber(pctChange(close, closes[i - 63])) : null,
      return_6m: i >= 126 ? roundNumber(pctChange(close, closes[i - 126])) : null,
      return_1y: i >= 252 ? roundNumber(pctChange(close, closes[i - 252])) : null,
      week_52_high: prior252High === null ? null : roundNumber(prior252High),
      week_52_low: prior252Low === null ? null : roundNumber(prior252Low),
      distance_from_52w_high_pct: roundNumber(distanceFrom52wHighPct),
      distance_from_52w_low_pct: roundNumber(distanceFrom52wLowPct),
      near_52w_high:
        distanceFrom52wHighPct === null
          ? null
          : distanceFrom52wHighPct <= 5,
      week_52_high_breakout:
        prior252High === null ? null : close >= prior252High,
      all_time_high: priorAth === null ? null : roundNumber(priorAth),
      distance_from_ath_pct: roundNumber(distanceFromAthPct),
      all_time_high_breakout:
        priorAth === null ? null : close >= priorAth,
      avg_volume_20d: i >= 19 ? roundNumber(volume20Sum / 20) : null,
      avg_traded_value_20d: i >= 19 ? roundNumber(tradedValue20Sum / 20) : null,
      avg_traded_value_50d: i >= 49 ? roundNumber(tradedValue50Sum / 50) : null,
      volume_ratio:
        i >= 19 && volume20Sum > 0 ? roundNumber(volume / (volume20Sum / 20)) : null,
      traded_days_20d: i >= 19 ? tradedDays20Sum : null,
      volatility_20d:
        i >= 20 && vol20Count === 20 ? roundNumber(stdDev(vol20Sum, vol20Sq, vol20Count) * 100) : null,
      volatility_50d:
        i >= 50 && vol50Count === 50 ? roundNumber(stdDev(vol50Sum, vol50Sq, vol50Count) * 100) : null,
      atr_14: i >= 14 ? atr14Arr[i] : null,
      atr_pct:
        i >= 14 && atr14Arr[i] !== null ? roundNumber(pctFromDelta(atr14Arr[i], close)) : null,
      rsi_14: rsi14Arr[i],
      macd_line: i >= 34 && macdLineArr[i] !== null ? roundNumber(macdLineArr[i]) : null,
      macd_signal: i >= 34 && macdSignalArr[i] !== null ? roundNumber(macdSignalArr[i]) : null,
      macd_histogram: i >= 34 && macdHistArr[i] !== null ? roundNumber(macdHistArr[i]) : null,
      adx_14: adx14Arr[i],
      supertrend: supertrendArr[i],
      supertrend_signal: supertrendSignalArr[i],
      higher_high_20d: i >= 19 ? high >= Math.max(...highs.slice(i - 19, i + 1)) : null,
      higher_low_20d: i >= 19 ? low > (low20Sum / 20) : null,
      is_liquid:
        i >= 19
          ? tradedDays20Sum >= INDICATOR_PARAMS.liquidity.tradedDays20dMin
            && (tradedValue20Sum / 20) >= INDICATOR_PARAMS.liquidity.avgTradedValue20dMin
          : null,
    };

    const dataQualityFlags = [];
    if (close <= 0) dataQualityFlags.push("non_positive_close");
    if (volume <= 0) dataQualityFlags.push("zero_volume");

    metrics.derived_meta = {
      obv: roundNumber(obvValues[i]),
      prior_252_high: prior252High === null ? null : roundNumber(prior252High),
      prior_252_low: prior252Low === null ? null : roundNumber(prior252Low),
      prior_ath: priorAth === null ? null : roundNumber(priorAth),
      dma_50_10d_ago: i >= 10 && dma50Arr[i - 10] !== null ? roundNumber(dma50Arr[i - 10]) : null,
      dma_200_10d_ago: i >= 10 && dma200Arr[i - 10] !== null ? roundNumber(dma200Arr[i - 10]) : null,
      indicator_params: INDICATOR_PARAMS,
      calculation_version: CALCULATION_VERSION,
      data_quality_flags: dataQualityFlags,
      missing_history_reasons: buildMissingHistoryReasons(i + 1, metrics),
      min_history_satisfied_by_field: {
        dma_20: i + 1 >= 20,
        dma_50: i + 1 >= 50,
        dma_200: i + 1 >= 200,
        week_52: i + 1 >= 253,
        rsi_14: i + 1 >= 28,
        macd: i + 1 >= 35,
        adx_14: i + 1 >= 28,
        atr_14: i + 1 >= 15,
      },
    };

    metricsByRow[i] = metrics;

    while (prior252HighDeque.length && highs[prior252HighDeque[prior252HighDeque.length - 1]] <= high) {
      prior252HighDeque.pop();
    }
    prior252HighDeque.push(i);

    while (prior252LowDeque.length && lows[prior252LowDeque[prior252LowDeque.length - 1]] >= low) {
      prior252LowDeque.pop();
    }
    prior252LowDeque.push(i);

    while (high20Deque.length && highs[high20Deque[high20Deque.length - 1]] <= high) {
      high20Deque.pop();
    }
    high20Deque.push(i);

    allTimePriorHigh = allTimePriorHigh === null ? high : Math.max(allTimePriorHigh, high);
  }

  return candles.map((candle, index) => ({
    master_id: Number(candle.master_id),
    trade_date: toTradeDateKey(candle.trade_date),
    ...metricsByRow[index],
  }));
}

module.exports = {
  CALCULATION_VERSION,
  INDICATOR_PARAMS,
  toTradeDateKey,
  computeDerivedMetricsForCandles,
};
