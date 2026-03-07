const express = require("express");
const router = express.Router();

const {
  fetchStockFundamentals,
  getStockFundamentalsBySymbol,
  enqueueFundamentalsJob,
  enqueueAllFundamentalsJobs,
  clearFundamentalsQueue,
  previewFundamentalsByName,
} = require("../controllers/stockFundamental.controller");

// Add to main list
router.post("/fetch", fetchStockFundamentals);

// get Stock fundamentals
router.get("/:symbol", getStockFundamentalsBySymbol);

// enqueue fundamentals job for a specific master stock
router.post("/queue", enqueueFundamentalsJob);

// enqueue fundamentals jobs for all master stocks
router.post("/queue/all", enqueueAllFundamentalsJobs);

// preview fundamentals by name (no DB write)
router.post("/preview", previewFundamentalsByName);

// clear fundamentals queue
router.post("/queue/clear", clearFundamentalsQueue);

module.exports = router;
