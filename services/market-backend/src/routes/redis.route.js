const express = require("express");
const router = express.Router();

const requireSuperAdmin = require("../middleware.js/requireSuperAdmin.middleware");
const {
  getRedisHealth,
  getRedisStatus,
  listRedisKeys,
  deleteRedisKeysByPattern,
  resetMarketRedis,
  flushRedisDb,
} = require("../controllers/redisAdmin.controller");

router.get("/health", requireSuperAdmin, getRedisHealth);
router.get("/status", requireSuperAdmin, getRedisStatus);
router.get("/keys", requireSuperAdmin, listRedisKeys);
router.post("/delete-keys", requireSuperAdmin, deleteRedisKeysByPattern);
router.post("/reset-market", requireSuperAdmin, resetMarketRedis);
router.post("/flushdb", requireSuperAdmin, flushRedisDb);

module.exports = router;
