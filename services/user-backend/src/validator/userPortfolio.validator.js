const { body, param } = require("express-validator");

/* ---------------- CREATE PORTFOLIO ---------------- */

exports.createPortfolioValidator = [
  body("portfolio_type_id")
    .notEmpty()
    .withMessage("portfolio_type_id is required"),

  body("name")
    .trim()
    .isLength({ min: 3, max: 50 })
    .withMessage("name must be between 3 and 50 characters"),

  body("initial_fund")
    .optional()
    .isFloat({ min: 0 })
    .withMessage("initial_fund must be 0 or greater"),

  body("meta")
    .optional()
    .isObject()
    .withMessage("meta must be an object"),

  body("meta.as_of_date")
    .optional()
    .isISO8601()
    .withMessage("meta.as_of_date must be a valid date"),

  body("meta.query")
    .optional()
    .isString()
    .withMessage("meta.query must be a string"),

  body("meta.watchlist_master_ids")
    .optional()
    .isArray()
    .withMessage("meta.watchlist_master_ids must be an array"),
];

exports.updateBacktestMetaValidator = [
  body("meta")
    .exists()
    .withMessage("meta is required")
    .bail()
    .isObject()
    .withMessage("meta must be an object"),

  body("meta.enabled_versions")
    .optional()
    .isArray()
    .withMessage("meta.enabled_versions must be an array"),
];

/* ---------------- PARAM ---------------- */

exports.portfolioIdParamValidator = [
  param("portfolioId")
    .notEmpty()
    .withMessage("portfolioId is required"),
];

/* ---------------- HOLDINGS BY ACTIVE STOCK ---------------- */

exports.activeStockIdParamValidator = [
  param("activeStockId")
    .notEmpty()
    .withMessage("activeStockId is required"),
];
