const express = require("express");
const router = express.Router();

const controller = require("../controllers/userWallet.controller");
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const {
  walletAmountValidator,
  walletTransferParamValidator,
  walletLedgerQueryValidator,
} = require("../validator/userWallet.validator");

router.post(
  "/load",
  authMiddleware,
  walletAmountValidator,
  validate,
  controller.loadWalletFund,
);

router.post(
  "/withdraw",
  authMiddleware,
  walletAmountValidator,
  validate,
  controller.withdrawWalletFund,
);

router.post(
  "/transfer-to-portfolio/:portfolioId",
  authMiddleware,
  walletTransferParamValidator,
  walletAmountValidator,
  validate,
  controller.transferWalletToPortfolio,
);

router.post(
  "/transfer-from-portfolio/:portfolioId",
  authMiddleware,
  walletTransferParamValidator,
  walletAmountValidator,
  validate,
  controller.transferPortfolioToWallet,
);

router.get(
  "/ledger",
  authMiddleware,
  walletLedgerQueryValidator,
  validate,
  controller.getWalletLedger,
);

module.exports = router;
