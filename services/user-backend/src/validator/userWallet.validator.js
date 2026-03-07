const { body, param, query } = require("express-validator");

exports.walletAmountValidator = [
  body("amount")
    .notEmpty()
    .withMessage("amount is required")
    .isFloat({ gt: 0 })
    .withMessage("amount must be greater than 0"),
];

exports.walletTransferParamValidator = [
  param("portfolioId")
    .notEmpty()
    .withMessage("portfolioId is required"),
];

exports.walletLedgerQueryValidator = [
  query("limit")
    .optional()
    .isInt({ min: 1, max: 200 })
    .withMessage("limit must be between 1 and 200"),
  query("skip")
    .optional()
    .isInt({ min: 0 })
    .withMessage("skip must be 0 or greater"),
];
