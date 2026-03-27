const { body, param } = require("express-validator");

const summarizeByNewsIdValidator = [
  body("newsId")
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const importantPointsValidator = [
  body("newsId")
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("summary")
    .isString()
    .withMessage("summary is required")
    .isLength({ min: 5, max: 40000 })
    .withMessage("summary must be between 5 and 40000 characters"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const generateScriptValidator = [
  body("newsId")
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("points")
    .isString()
    .withMessage("points is required")
    .isLength({ min: 5, max: 60000 })
    .withMessage("points must be between 5 and 60000 characters"),
  body("language")
    .optional()
    .isIn(["english", "hindi"])
    .withMessage("language must be either english or hindi"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const generateStructuredScriptValidator = [
  body("newsId")
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("points")
    .isString()
    .withMessage("points is required")
    .isLength({ min: 5, max: 60000 })
    .withMessage("points must be between 5 and 60000 characters"),
  body("language")
    .optional()
    .isIn(["english", "hindi"])
    .withMessage("language must be either english or hindi"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
  body("targetDurationSec")
    .optional()
    .isInt({ min: 30, max: 900 })
    .withMessage("targetDurationSec must be between 30 and 900"),
  body("wordCountMin")
    .optional()
    .isInt({ min: 100, max: 5000 })
    .withMessage("wordCountMin must be between 100 and 5000"),
  body("wordCountMax")
    .optional()
    .isInt({ min: 120, max: 6000 })
    .withMessage("wordCountMax must be between 120 and 6000"),
];

const generateScriptAudioValidator = [
  body("newsId")
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("script")
    .isString()
    .withMessage("script is required")
    .isLength({ min: 5, max: 120000 })
    .withMessage("script must be between 5 and 120000 characters"),
  body("language")
    .optional()
    .isIn(["english", "hindi"])
    .withMessage("language must be either english or hindi"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const extractHighlightTermsValidator = [
  body("newsId")
    .optional()
    .isInt({ min: 1 })
    .withMessage("newsId must be a positive integer"),
  body("script")
    .isString()
    .withMessage("script is required")
    .isLength({ min: 5, max: 120000 })
    .withMessage("script must be between 5 and 120000 characters"),
  body("language")
    .optional()
    .isIn(["english", "hindi"])
    .withMessage("language must be either english or hindi"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const startBatchValidator = [
  body("date")
    .isString()
    .withMessage("date is required")
    .matches(/^\d{4}-\d{2}-\d{2}$/)
    .withMessage("date must be in YYYY-MM-DD format"),
  body("category")
    .optional()
    .isString()
    .withMessage("category must be a string")
    .isLength({ max: 120 })
    .withMessage("category must be up to 120 characters"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
  body("gapMs")
    .optional()
    .isInt({ min: 0, max: 10000 })
    .withMessage("gapMs must be between 0 and 10000"),
];

const batchStatusValidator = [
  param("jobId")
    .isString()
    .withMessage("jobId is required")
    .isLength({ min: 6, max: 100 })
    .withMessage("jobId is invalid"),
];

const stopBatchValidator = [...batchStatusValidator];

module.exports = {
  summarizeByNewsIdValidator,
  importantPointsValidator,
  generateScriptValidator,
  generateStructuredScriptValidator,
  generateScriptAudioValidator,
  extractHighlightTermsValidator,
  startBatchValidator,
  batchStatusValidator,
  stopBatchValidator,
};
