const express = require("express");
const router = express.Router();

const {
  getHistoricalUniverseRules,
  filterHistoricalUniverse,
  getHistoricalUniverseFilterCacheState,
  searchHistoricalUniverse,
  searchHistoricalUniverseUsingSplitData,
  searchHistoricalUniverseUsingSplitDataFast,
} = require("../controllers/historicalUniverse.controller");

router.get("/rules", getHistoricalUniverseRules);
router.post("/filter/cache", getHistoricalUniverseFilterCacheState);
router.post("/filter", filterHistoricalUniverse);
router.post("/search", searchHistoricalUniverse);
router.post("/search-split", searchHistoricalUniverseUsingSplitData);
router.post("/search-split-fast", searchHistoricalUniverseUsingSplitDataFast);

module.exports = router;
