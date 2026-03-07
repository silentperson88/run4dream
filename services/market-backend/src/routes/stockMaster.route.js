const express = require("express");
const router = express.Router();

const {
  createrMasterStock,
  getMasterList,
} = require("../controllers/stockMaster.controller");

// Add to main list
router.patch("/create", createrMasterStock);

// get Stock fundamentals list by pagination
router.get("/", getMasterList);

module.exports = router;
