const redis = require("../config/redis.config");
const MarketState = require("../enums/marketState.enum");
const MARKET_CONFIG = require("../config/market.config");
const delay = require("../utils/delay.util");
const {
  prepareRedisForMarket,
} = require("../services/redisPreparation.service");
const { runFullMode } = require("../services/fullMode.service");
const { runLtcBasicMode } = require("../services/ltpMode.service");
const { getLastEntry } = require("../services/token.service");
const { isSameDay, isMarketClosed } = require("../utils/Mthods.utils");
require("../utils/constants.utils");
const generateMarketSnapshot = require("../utils/generateMarketSnapshot");
const { REDIS_KEYS } = require("../utils/Constants/redisKey.consants");

const { TOKEN_RETRY_DELAY_MS, TOKEN_MAX_RETRIES, CYCLE_DELAY_MS } =
  MARKET_CONFIG;

function isResumableState(state) {
  return [
    MarketState.FULL_DONE,
    MarketState.LTC_WAIT,
    MarketState.LTC_RUNNING,
    MarketState.OHLC_RUNNING,
    MarketState.OHLC_DONE,
  ].includes(state);
}

function isTerminalState(state) {
  return [MarketState.DONE, MarketState.MARKET_CLOSED].includes(state);
}

/**
 * Dummy token checker (replace later with real API / DB logic)
 */
async function isTokenReadyForToday() {
  // TEMP (for now)
  try {
    const loginData = await getLastEntry();

    if (!loginData) {
      return false;
    }

    const now = Date.now();

    const generatedAt = new Date(loginData.generated_at).getTime();
    const expiryAt = new Date(loginData.expiry_time).getTime();

    const isToday = isSameDay(generatedAt, now);
    const isExpired = !isToday || now > expiryAt;

    if (isExpired) {
      await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.WAITING_FOR_TOKEN);
      return false;
    }

    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.TOKEN_READY);
    await redis.set(
      REDIS_KEYS.MARKET_TIME,
      JSON.stringify(loginData.market || {}),
    );
    return true;
  } catch (error) {
    console.error("Token check failed", error);
    return false; // ❗ scheduler must not crash
  }
}

/**
 * Initialize market day
 */
async function initializeMarketDay() {
  const today = new Date().toISOString().split("T")[0];
  const now = Date.now();

  console.log(`Initializing market for ${today}`);

  const existingDate = await redis.get(REDIS_KEYS.MARKET_DATE);
  const existingState = await redis.get(REDIS_KEYS.MARKET_STATE);

  // 🔁 If same day & already initialized, do not wipe data
  if (existingDate === today && existingState) {
    console.log("Market already initialized for today");
    return;
  }

  /* -------------------------------
     SET DAY-SCOPED DATA (NO CLEAR)
  --------------------------------*/

  const pipeline = redis.pipeline();

  pipeline.set(REDIS_KEYS.MARKET_DATE, today);
  pipeline.set(REDIS_KEYS.MARKET_STATE, MarketState.INIT);
  pipeline.set(REDIS_KEYS.MARKET_STARTED_AT, now);
  pipeline.set(REDIS_KEYS.MARKET_LAST_RESET, now);
  pipeline.set(REDIS_KEYS.MARKET_IS_RUNNING, false);
  pipeline.set(REDIS_KEYS.MARKET_MINUTE_COUNTER, 0);
  pipeline.set(REDIS_KEYS.MARKET_LAST_CYCLE_AT, 0);
  pipeline.set(REDIS_KEYS.MARKET_FULL_DONE, false);

  await pipeline.exec();

  console.log(`Market initialized for ${today}`);
}

function parseMarketTime(marketTimeStr) {
  const [hh, mm] = marketTimeStr.split(":").map(Number);
  const d = new Date();
  d.setHours(hh, mm, 0, 0);
  return d;
}

async function waitUntil(timestampMs) {
  const now = Date.now();
  if (timestampMs <= now) return;
  await delay(timestampMs - now);
}

/**
 * Main scheduler logic
 */
async function runMarketScheduler() {
  console.log("Market scheduler started");
  await initializeMarketDay();

  const today = new Date().toISOString().split("T")[0];
  const existingDate = await redis.get(REDIS_KEYS.MARKET_DATE);
  let currentState = await redis.get(REDIS_KEYS.MARKET_STATE);

  // If market is already closed or completed for today, do nothing
  if (existingDate === today && isTerminalState(currentState)) {
    console.log(`Market already ${currentState} for today. Exiting.`);
    return;
  }

  // Wait until 09:00 before checking token
  const tokenCheckStart = new Date();
  tokenCheckStart.setHours(9, 0, 0, 0);
  await waitUntil(tokenCheckStart.getTime());

  // If no state is set for today, start waiting for token
  if (!currentState || existingDate !== today) {
    await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.WAITING_FOR_TOKEN);
    currentState = MarketState.WAITING_FOR_TOKEN;
  }

  let retryCount = 0;
  let cycleComplete = false;
  let fullModeCompletedThisBoot = false;

  while (retryCount < TOKEN_MAX_RETRIES && !cycleComplete) {
    const tokenReady = await isTokenReadyForToday();
    if (await isMarketClosed()) return;
    if (tokenReady) {
      console.log("Token ready, proceeding");
      currentState = await redis.get(REDIS_KEYS.MARKET_STATE);

      const marketTimeRaw = await redis.get(REDIS_KEYS.MARKET_TIME);
      const marketTime = marketTimeRaw ? JSON.parse(marketTimeRaw) : null;
      if (!marketTime?.open_time || !marketTime?.close_time) {
        throw new Error("Market time not found in Redis");
      }

      const openAt = parseMarketTime(marketTime.open_time);
      const closeAt = parseMarketTime(marketTime.close_time);

      const now = new Date();
      if (now > closeAt) {
        console.log("Market already closed. Exiting.");
        await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.MARKET_CLOSED);
        return;
      }

      const fullDoneRaw = await redis.get(REDIS_KEYS.MARKET_FULL_DONE);
      const fullDone = fullDoneRaw === "true";

      if (!fullModeCompletedThisBoot && now < closeAt) {
        // Always refresh Redis active stock universe before FULL on server boot.
        // If market not opened yet, do it just before open; otherwise do it immediately.
        if (now < openAt) {
          const resetAt = openAt.getTime() - 30_000;
          await waitUntil(resetAt);
        }

        await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.PREPARING);
        await prepareRedisForMarket();

        await waitUntil(openAt.getTime());
        await runFullMode();
        fullModeCompletedThisBoot = true;
        await redis.set(REDIS_KEYS.MARKET_FULL_DONE, true);
        await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.FULL_DONE);
      } else {
        console.log(`Skipping FULL mode (fullDone=${fullDone}, fullModeCompletedThisBoot=${fullModeCompletedThisBoot})`);
      }

      const cycleMinutes = CYCLE_DELAY_MS / 1000 / 60;

      await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.LTC_RUNNING);
      await redis.set(REDIS_KEYS.MARKET_IS_RUNNING, true);

      let minuteCounter = parseInt(
        (await redis.get(REDIS_KEYS.MARKET_MINUTE_COUNTER)) || "0",
        10,
      );
      let lastCycleAt = parseInt(
        (await redis.get(REDIS_KEYS.MARKET_LAST_CYCLE_AT)) || "0",
        10,
      );

      while (Date.now() < closeAt.getTime()) {
        const nowMs = Date.now();
        if (lastCycleAt && nowMs - lastCycleAt < CYCLE_DELAY_MS) {
          await delay(CYCLE_DELAY_MS - (nowMs - lastCycleAt));
        }

        minuteCounter++;

        const shouldUpdateDb = minuteCounter % 5 === 0;
        await runLtcBasicMode({
          setState: false,
          broadcast: true,
          updateDb: shouldUpdateDb,
        });

        lastCycleAt = Date.now();
        await redis.set(REDIS_KEYS.MARKET_MINUTE_COUNTER, minuteCounter);
        await redis.set(REDIS_KEYS.MARKET_LAST_CYCLE_AT, lastCycleAt);

        console.log(`Waiting ${cycleMinutes} min for next cycle`);
        await delay(CYCLE_DELAY_MS);
      }

      // Final DB update on close
      await runLtcBasicMode({
        setState: false,
        broadcast: true,
        updateDb: true,
      });

      await redis.set(REDIS_KEYS.MARKET_IS_RUNNING, false);
      await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.DONE);
      console.log("Market day completed");

      //   One the market is closed then generate 1 file wth all stock data in json format
      await generateMarketSnapshot();

      cycleComplete = true;

      return;
    }

    retryCount++;
    console.log(`Token not ready, retry ${retryCount}/${TOKEN_MAX_RETRIES}`);
    await delay(TOKEN_RETRY_DELAY_MS);
  }

  console.error("Token not ready after max retries. Aborting.");
  await redis.set(REDIS_KEYS.MARKET_STATE, MarketState.DONE);
}

/**
 * Exported start function
 */
async function startMarketScheduler() {
  await runMarketScheduler();
}

module.exports = {
  startMarketScheduler,
};


