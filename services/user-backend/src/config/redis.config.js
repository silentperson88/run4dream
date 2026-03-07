const Redis = require("ioredis");

const { REDIS_HOST, REDIS_PORT } = process.env;

const redis = new Redis({
  host: REDIS_HOST || "localhost",
  port: REDIS_PORT || 6379,
  maxRetriesPerRequest: null,
});

redis.on("connect", () => {
  console.log("🟢 Redis connected");
});

redis.on("error", (err) => {
  console.error("🔴 Redis error", err);
});

module.exports = redis;
