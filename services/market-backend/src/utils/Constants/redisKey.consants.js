// Redis keys
module.exports.REDIS_KEYS = {
  STOCK_SNAPSHOT: "stock:snapshot:",
  STOCKS_ACTIVE: "stocks:active",
  STOCKS_TOKEN: "stocks:token:",
  MARKET_STATE: "market:state",
  MARKET_TIME: "market:marketTime",
  MARKET_DATE: "market:date",
  MARKET_STARTED_AT: "market:startedAt",
  MARKET_LAST_RESET: "market:lastReset",
  MARKET_SCHEDULER_LOCK: "market:scheduler:lock",
  MARKET_SCHEDULER_HEARTBEAT: "market:scheduler:heartbeat",
  MARKET_IS_RUNNING: "market:isRunning",
  MARKET_MINUTE_COUNTER: "market:minuteCounter",
  MARKET_LAST_CYCLE_AT: "market:lastCycleAt",
  MARKET_FULL_DONE: "market:fullDone",
  FUNDAMENTALS_QUEUE: "fundamentals:queue",
  FUNDAMENTALS_DEDUPE: "fundamentals:queue:dedupe",
};
