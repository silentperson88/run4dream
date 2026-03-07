const { MARKET_CLOSE_TIME } = require("../config/market.config");
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("./Constants/redisKey.consants");

exports.isSameDay = (t1, t2) => {
  const d1 = new Date(t1);
  const d2 = new Date(t2);

  return (
    d1.getFullYear() === d2.getFullYear() &&
    d1.getMonth() === d2.getMonth() &&
    d1.getDate() === d2.getDate()
  );
};

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

exports.toSmartApiDate = (date, time = "09:15") => {
  return `${date} ${time}`;
};

exports.normalizeEodDate = (timestamp) => {
  const d = new Date(timestamp);
  d.setHours(0, 0, 0, 0);
  return d;
};
