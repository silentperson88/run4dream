const walletService = require("../services/userWallet.service");
const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");

async function loadWalletFund(req, res) {
  try {
    const data = await walletService.loadWalletFund({
      user_id: req.user.id,
      amount: Number(req.body.amount),
      source: req.body.source || "WALLET_LOAD",
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

async function transferWalletToPortfolio(req, res) {
  try {
    const data = await walletService.transferWalletToPortfolio({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
      amount: Number(req.body.amount),
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

async function transferPortfolioToWallet(req, res) {
  try {
    const data = await walletService.transferPortfolioToWallet({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
      amount: Number(req.body.amount),
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

async function getWalletLedger(req, res) {
  try {
    const data = await walletService.getWalletLedger({
      user_id: req.user.id,
      limit: Number(req.query.limit || 50),
      skip: Number(req.query.skip || 0),
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

async function withdrawWalletFund(req, res) {
  try {
    const data = await walletService.withdrawWalletFund({
      user_id: req.user.id,
      amount: Number(req.body.amount),
      source: req.body.source || "WALLET_WITHDRAW",
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

module.exports = {
  loadWalletFund,
  transferWalletToPortfolio,
  transferPortfolioToWallet,
  getWalletLedger,
  withdrawWalletFund,
};
