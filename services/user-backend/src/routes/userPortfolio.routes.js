const express = require("express");
const router = express.Router();

const controller = require("../controllers/userPortfolio.controller");
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const {
  createPortfolioValidator,
  portfolioIdParamValidator,
  activeStockIdParamValidator,
} = require("../validator/userPortfolio.validator");

router.post(
  "/",
  authMiddleware,
  createPortfolioValidator,
  validate,
  controller.createPortfolio,
);

router.get("/", authMiddleware, controller.getPortfolios);

router.get(
  "/:portfolioId",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  controller.getPortfolioById,
);

router.get(
  "/summary",
  authMiddleware,
  controller.getAllPortfoliosSummary,
);

router.get(
  "/:portfolioId/summary",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  controller.getPortfolioSummary,
);

router.get(
  "/:portfolioId/with-orders",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  controller.getPortfolioWithOrders,
);

router.get(
  "/:portfolioId/holdings",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  controller.getPortfolioHoldings,
);

router.get(
  "/:portfolioId/holdings/:activeStockId/orders",
  authMiddleware,
  portfolioIdParamValidator,
  activeStockIdParamValidator,
  validate,
  controller.getPortfolioHoldingOrders,
);

router.get(
  "/holdings/:activeStockId",
  authMiddleware,
  activeStockIdParamValidator,
  validate,
  controller.getHoldingsByActiveStock,
);

router.patch(
  "/:portfolioId/archive",
  authMiddleware,
  portfolioIdParamValidator,
  validate,
  controller.archivePortfolio,
);

module.exports = router;
