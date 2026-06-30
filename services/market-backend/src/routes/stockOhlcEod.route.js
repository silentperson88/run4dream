const express = require("express");
const router = express.Router();

const {
  fetchEodByRange,
  fetchEodByRangePreview,
  fetchEodByRangeChunked,
  getEodFromDbByRange,
  syncDailyEodFromFull,
  triggerDailyEodFromFull,
} = require("../controllers/stockOhlcEod.controller");
const {
  fetchEodByRangeValidationRule,
  getEodFromDbValidationRule,
  syncDailyEodFromFullValidationRule,
} = require("../validator/stockOhlcEod.validator");
const validateRequest = require("../middleware.js/validateRequest.middleware");

router.post(
  "/fetch-by-range-preview",
  fetchEodByRangeValidationRule,
  validateRequest,
  fetchEodByRangePreview,
);

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

router.post(
  "/sync-daily-full",
  syncDailyEodFromFullValidationRule,
  validateRequest,
  syncDailyEodFromFull,
);

router.post(
  "/sync-daily-full-trigger",
  syncDailyEodFromFullValidationRule,
  validateRequest,
  triggerDailyEodFromFull,
);

module.exports = router;
