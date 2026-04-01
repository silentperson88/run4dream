const express = require("express");
const router = express.Router();

const {
  getAllRawStocks,
  getRawStockPrice,
  createRawStock,
  updateRawStockStatus,
} = require("../controllers/adminRawStock.controller");
const { getStockUniverseAudit, markStockUniverseFilteredInactive } = require("../controllers/stockUniverseAudit.controller");
const { activateStock } = require("../controllers/adminActiveStock.controller");
const {
  loginWithTOTP,
  checkLoginStatus,
} = require("../controllers/smartapilogin.controller");
const { resetMarketRedis } = require("../controllers/marketAdmin.controller");
const { createEodFromRedis } = require("../controllers/eodAdmin.controller");
const requireSuperAdmin = require("../middleware.js/requireSuperAdmin.middleware");

// handle login using totp
router.post("/smart-login", requireSuperAdmin, loginWithTOTP);

// check login status
router.get("/server-status", checkLoginStatus);

// Raw stock listing
router.get("/raw-stocks", getAllRawStocks);
router.post("/raw-stock", createRawStock);
router.get("/stock-universe-audit", getStockUniverseAudit);
router.post("/stock-universe-audit/mark-inactive", markStockUniverseFilteredInactive);

// get raw stock price
router.post("/raw-stock-price", getRawStockPrice);

// update raw stock status
router.patch("/raw-stock/status", updateRawStockStatus);

// Add to main list
router.post("/activate-stock", activateStock);

// Reset market redis (manual recovery)
router.post("/reset-redis", resetMarketRedis);

// Create EOD records from Redis snapshot
router.post("/eod-from-redis", createEodFromRedis);

module.exports = router;
