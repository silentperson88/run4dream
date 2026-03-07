const { body } = require("express-validator");
const { MESSAGES } = require("../utils/constants/response.constants");

const createPortfolioValidator = [
  body("portfolio_type_id").notEmpty().withMessage(MESSAGES.PORTFOLIO.TYPE_REQUIRED),
  body("name").trim().notEmpty().withMessage(MESSAGES.PORTFOLIO.NAME_REQUIRED),
  body("initial_fund").optional().isFloat({ min: 0 }).withMessage(MESSAGES.PORTFOLIO.INITIAL_FUND_INVALID),
];

module.exports = { createPortfolioValidator };
