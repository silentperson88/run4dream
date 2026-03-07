const MARKET_CONFIG = {
  MARKET_OPEN_TIME: "09:15",
  //   MARKET_CLOSE_TIME: "11:40",
  MARKET_CLOSE_TIME: "15:30",

  FULL_MODE_BATCH_SIZE: 50,
  LTC_MODE_BATCH_SIZE: 50,
  OHLC_MODE_BATCH_SIZE: 50,

  //   Delay between each batch API call to avoid rate limits
  API_BATCH_DELAY_MS: 20_000, // 20 sec

  //   Delay between each LTP cycle to repeat LTP fetches to ensure data consistency
  LTC_CYCLE_DELAY_MS: 2 * 60_000, // 2 min

  //   Delay between each cycle of FULL → LTC → OHLC
  // CYCLE_DELAY_MS: 20_000, // 1 min
  CYCLE_DELAY_MS: 60_000, // 1 min

  TOKEN_RETRY_DELAY_MS: 5 * 60_000, // 5 min
  TOKEN_MAX_RETRIES: 10,
};

module.exports = MARKET_CONFIG;
