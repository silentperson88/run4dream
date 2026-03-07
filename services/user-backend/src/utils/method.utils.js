const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("./constants/redis.constants");

exports.isMarketClosed = async () => {
  const now = new Date();
  const market = await redis.get(REDIS_KEYS.MARKET_TIME);
  console.log("Market open time:", market, "now:", now);
  const [hh, mm] = JSON.parse(market)
    .close_time.toString()
    .split(":")
    .map(Number);

  const marketClose = new Date();
  marketClose.setHours(hh, mm, 0, 0);
  console.log("Market close time:", marketClose, now >= marketClose);

  return now >= marketClose;
};
