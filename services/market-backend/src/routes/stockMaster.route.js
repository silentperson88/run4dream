const express = require("express");
const router = express.Router();

const {
  createrMasterStock,
  getMasterList,
  updateTokenAndExchange,
  markMasterInactive,
} = require("../controllers/stockMaster.controller");

// Add to main list
router.patch("/create", createrMasterStock);
router.patch("/:id/token-exchange", updateTokenAndExchange);
router.patch("/:id/inactive", markMasterInactive);

// get Stock fundamentals list by pagination
router.get("/", getMasterList);

module.exports = router;
