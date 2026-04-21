const express = require("express");
const router = express.Router();

const {
  getHistoricalUniverseRules,
  filterHistoricalUniverse,
  searchHistoricalUniverse,
  searchHistoricalUniverseUsingSplitData,
} = require("../controllers/historicalUniverse.controller");

router.get("/rules", getHistoricalUniverseRules);
router.post("/filter", filterHistoricalUniverse);
router.post("/search", searchHistoricalUniverse);
router.post("/search-split", searchHistoricalUniverseUsingSplitData);

module.exports = router;
