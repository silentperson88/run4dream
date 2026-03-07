// src/routes/order.routes.js
const express = require("express");
const {
  placeOrder,
  getOpenOrders,
  getOpenOrdersByPortfolio,
} = require("../controllers/order.controller");
const {
  placeOrderValidator,
  portfolioIdParamValidator,
} = require("../validator/order.validator");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");

const router = express.Router();

router.post(
  "/place",
  authMiddleware,
  placeOrderValidator,
  validate,
  placeOrder,
);

router.get("/open", authMiddleware, getOpenOrders);

router.get(
  "/open/:portfolioId",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  getOpenOrdersByPortfolio,
);

module.exports = router;
