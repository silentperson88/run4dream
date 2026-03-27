const express = require("express");
const router = express.Router();
const controller = require("../controllers/newsApproach.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const {
  summarizeByNewsIdValidator,
  importantPointsValidator,
  generateScriptValidator,
  generateStructuredScriptValidator,
  generateScriptAudioValidator,
  extractHighlightTermsValidator,
  startBatchValidator,
  batchStatusValidator,
  stopBatchValidator,
} = require("../validator/newsApproach.validator");

router.post(
  "/summarize-by-news-id",
  authMiddleware,
  summarizeByNewsIdValidator,
  validate,
  controller.summarizeByNewsId,
);

router.post(
  "/important-points",
  authMiddleware,
  importantPointsValidator,
  validate,
  controller.importantPoints,
);

router.post(
  "/generate-script",
  authMiddleware,
  generateScriptValidator,
  validate,
  controller.generateScript,
);

router.post(
  "/generate-structured-script",
  authMiddleware,
  generateStructuredScriptValidator,
  validate,
  controller.generateStructuredScript,
);

router.post(
  "/generate-script-audio",
  authMiddleware,
  generateScriptAudioValidator,
  validate,
  controller.generateScriptAudio,
);

router.post(
  "/extract-highlight-terms",
  authMiddleware,
  extractHighlightTermsValidator,
  validate,
  controller.extractHighlightTerms,
);

router.post(
  "/start-batch",
  authMiddleware,
  startBatchValidator,
  validate,
  controller.startBatch,
);

router.get(
  "/batch-status/:jobId",
  authMiddleware,
  batchStatusValidator,
  validate,
  controller.getBatchStatus,
);

router.post(
  "/stop-batch/:jobId",
  authMiddleware,
  stopBatchValidator,
  validate,
  controller.stopBatch,
);

router.get(
  "/assets/audio/:fileName",
  controller.streamNewsApproachAudio,
);

module.exports = router;
