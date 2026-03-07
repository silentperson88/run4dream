// src/validators/order.validator.js
const { body, param } = require("express-validator");

const placeOrderValidator = [
  body("portfolio_id").notEmpty().withMessage("portfolio_id is required"),
  body("active_stock_id").notEmpty().withMessage("active_stock_id is required"),
  body("type").isIn(["BUY", "SELL"]),
  body("order_type").isIn(["MARKET", "LIMIT"]),
  body("quantity").isInt({ min: 1 }),
  body("price").optional().isFloat({ min: 0 }),
];

const portfolioIdParamValidator = [
  param("portfolioId").notEmpty().withMessage("portfolioId is required"),
];

module.exports = { placeOrderValidator, portfolioIdParamValidator };
