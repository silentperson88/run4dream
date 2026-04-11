const express = require("express");
const router = express.Router();

const {
  fetchStockFundamentals,
  getStockFundamentalsBySymbol,
  getOverviewStockFundamentalsBySymbol,
  getQuarterlyStockFundamentalsBySymbol,
  getProfitLossStockFundamentalsBySymbol,
  getBalanceSheetStockFundamentalsBySymbol,
  getCashFlowStockFundamentalsBySymbol,
  getRatiosStockFundamentalsBySymbol,
  getShareholdingStockFundamentalsBySymbol,
  getDividendAnalysis,
  getDividendAnalysisBySymbol,
  getGrowthAnalysis,
  getGrowthAnalysisBySymbol,
  getGarpAnalysis,
  getGarpAnalysisBySymbol,
  getValueAnalysis,
  getValueAnalysisBySymbol,
  getPivotAnalysis,
  getPivotAnalysisBySymbol,
  getStockSearchSuggestions,
  searchStocks,
  enqueueFundamentalsJob,
  enqueueAllFundamentalsJobs,
  clearFundamentalsQueue,
  previewFundamentalsByName,
} = require("../controllers/stockFundamental.controller");

// Add to main list
router.post("/fetch", fetchStockFundamentals);

// get overview-only stock fundamentals
router.get("/overview/:symbol", getOverviewStockFundamentalsBySymbol);

// get quarterly performance stock fundamentals
router.get("/quarterly/:symbol", getQuarterlyStockFundamentalsBySymbol);

router.get("/profit-loss/:symbol", getProfitLossStockFundamentalsBySymbol);
router.get("/balance-sheet/:symbol", getBalanceSheetStockFundamentalsBySymbol);
router.get("/cash-flow/:symbol", getCashFlowStockFundamentalsBySymbol);
router.get("/ratios/:symbol", getRatiosStockFundamentalsBySymbol);
router.get("/shareholding/:symbol", getShareholdingStockFundamentalsBySymbol);
router.get("/analysis/dividend", getDividendAnalysis);
router.get("/analysis/dividend/:symbol", getDividendAnalysisBySymbol);
router.get("/analysis/growth", getGrowthAnalysis);
router.get("/analysis/growth/:symbol", getGrowthAnalysisBySymbol);
router.get("/analysis/garp", getGarpAnalysis);
router.get("/analysis/garp/:symbol", getGarpAnalysisBySymbol);
router.get("/analysis/value", getValueAnalysis);
router.get("/analysis/value/:symbol", getValueAnalysisBySymbol);
router.get("/analysis/pivot", getPivotAnalysis);
router.get("/analysis/pivot/:symbol", getPivotAnalysisBySymbol);
router.get("/analysis/search/suggestions", getStockSearchSuggestions);
router.get("/analysis/search", searchStocks);

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
