const portfolioService = require("../services/userPortfolio.service");
const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");

/* ---------------- CREATE ---------------- */

async function createPortfolio(req, res) {
  try {
    const data = await portfolioService.createUserPortfolio({
      user_id: req.user.id,
      ...req.body,
    });

    return response(res, 201, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

/* ---------------- LIST ---------------- */

async function getPortfolios(req, res) {
  try {
    console.log(" req.user.id", req.user);
    const data = await portfolioService.getUserPortfolios(req.user.id);
    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- GET ONE ---------------- */

async function getPortfolioById(req, res) {
  try {
    const data = await portfolioService.getUserPortfolioById({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
    });

    if (!data) {
      return response(res, 404, "Portfolio not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- GET ONE WITH ORDERS ---------------- */

async function getPortfolioWithOrders(req, res) {
  try {
    const data = await portfolioService.getUserPortfolioWithOrders({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
    });

    if (!data) {
      return response(res, 404, "Portfolio not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- HOLDINGS BY ACTIVE STOCK ---------------- */

async function getHoldingsByActiveStock(req, res) {
  try {
    const data = await portfolioService.getHoldingsByActiveStock({
      user_id: req.user.id,
      active_stock_id: req.params.activeStockId,
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- HOLDINGS BY PORTFOLIO ---------------- */

async function getPortfolioHoldings(req, res) {
  try {
    const data = await portfolioService.getPortfolioHoldings({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
    });

    if (!data) {
      return response(res, 404, "Portfolio not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- HOLDING ORDERS BY PORTFOLIO + STOCK ---------------- */

async function getPortfolioHoldingOrders(req, res) {
  try {
    const data = await portfolioService.getPortfolioHoldingOrders({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
      active_stock_id: req.params.activeStockId,
    });

    if (!data) {
      return response(res, 404, "Portfolio not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- SUMMARY: ALL PORTFOLIOS ---------------- */

async function getAllPortfoliosSummary(req, res) {
  try {
    const data = await portfolioService.getAllPortfoliosSummary({
      user_id: req.user.id,
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- SUMMARY: SINGLE PORTFOLIO ---------------- */

async function getPortfolioSummary(req, res) {
  try {
    const data = await portfolioService.getPortfolioSummary({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
    });

    if (!data) {
      return response(res, 404, "Portfolio not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

/* ---------------- ARCHIVE ---------------- */

async function archivePortfolio(req, res) {
  try {
    await portfolioService.archiveUserPortfolio({
      user_id: req.user.id,
      portfolio_id: req.params.portfolioId,
    });

    return response(res, 200, "Portfolio archived successfully");
  } catch (err) {
    return response(res, 400, err.message);
  }
}

module.exports = {
  createPortfolio,
  getPortfolios,
  getPortfolioById,
  getPortfolioWithOrders,
  getHoldingsByActiveStock,
  getPortfolioHoldings,
  getPortfolioHoldingOrders,
  getAllPortfoliosSummary,
  getPortfolioSummary,
  archivePortfolio,
};
