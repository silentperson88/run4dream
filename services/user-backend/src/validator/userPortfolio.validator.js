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
