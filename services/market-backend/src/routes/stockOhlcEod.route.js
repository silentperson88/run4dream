const express = require("express");
const router = express.Router();

const {
  fetchEodByRange,
  fetchEodByRangeChunked,
  getEodFromDbByRange,
} = require("../controllers/stockOhlcEod.controller");
const {
  fetchEodByRangeValidationRule,
  getEodFromDbValidationRule,
} = require("../validator/stockOhlcEod.validator");
const validateRequest = require("../middleware.js/validateRequest.middleware");

router.post(
  "/fetch-by-range",
  fetchEodByRangeValidationRule,
  validateRequest,
  fetchEodByRange,
);

router.post(
  "/fetch-by-range-chunked",
  fetchEodByRangeValidationRule,
  validateRequest,
  fetchEodByRangeChunked,
);

router.get(
  "/master/:master_id",
  getEodFromDbValidationRule,
  validateRequest,
  getEodFromDbByRange,
);

module.exports = router;
