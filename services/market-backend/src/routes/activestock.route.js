const express = require("express");
const router = express.Router();
const activeStockController = require("../controllers/activeStock.controller");


// Get all active stocks
router.get("/", activeStockController.getActiveStocksList);

// Get single active stock by token
router.get("/:token", activeStockController.getActiveStockByToken);

// Update live price / OHLC data
router.put(
  "/:token/update-price",
  activeStockController.updateActiveStockPrice
);

// Enable / Disable active stock
router.put("/:token/toggle", activeStockController.toggleActiveStock);

// Remove stock from active list
router.delete("/:token", activeStockController.deleteActiveStock);

module.exports = router;
