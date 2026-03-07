const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/constants/redis.constants");
const { MESSAGES } = require("../utils/constants/response.constants");
const { withTransaction } = require("../repositories/tx");
const userRepo = require("../repositories/users.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const orderRepo = require("../repositories/orders.repository");
const activeStockRepo = require("../repositories/activeStocks.repository");
const portfolioTypeRepo = require("../repositories/portfolioTypes.repository");
const { toNumber } = require("../repositories/common");

const attachPortfolioTypes = async (portfolios = []) => {
  if (!portfolios.length) return portfolios;

  const typeIds = Array.from(
    new Set(
      portfolios
        .map((p) => Number(p.portfolio_type_id))
        .filter((v) => Number.isFinite(v) && v > 0),
    ),
  );

  if (!typeIds.length) return portfolios;

  const types = await portfolioTypeRepo.getByIds(typeIds);
  const typeMap = Object.fromEntries(types.map((t) => [String(t.id), t]));

  return portfolios.map((p) => ({
    ...p,
    portfolio_type_id: typeMap[String(p.portfolio_type_id)] || p.portfolio_type_id,
  }));
};

async function createUserPortfolio({ user_id, portfolio_type_id, name, initial_fund = 0 }) {
  return withTransaction(async (client) => {
    const exists = await portfolioRepo.findActiveByUserAndName(user_id, name, client);
    if (exists) {
      throw new Error(MESSAGES.PORTFOLIO.ALREADY_EXISTS);
    }

    const fundToAllocate = Number(initial_fund || 0);
    if (fundToAllocate < 0) {
      throw new Error("initial_fund must be 0 or greater");
    }

    const type = await portfolioTypeRepo.getActiveById(portfolio_type_id, client);
    if (!type) {
      throw new Error(MESSAGES.PORTFOLIO.TYPE_NOT_FOUND);
    }

    let user = null;
    if (fundToAllocate > 0) {
      user = await userRepo.getById(user_id, client, { forUpdate: true });
      if (!user || !user.isActive) {
        throw new Error("User not found or inactive");
      }

      if (user.wallet_fund < fundToAllocate) {
        throw new Error("Insufficient wallet fund");
      }

      user.wallet_fund -= fundToAllocate;
      user.wallet_ledger = Array.isArray(user.wallet_ledger) ? user.wallet_ledger : [];
      user.wallet_ledger.push({
        type: "DEBIT",
        amount: fundToAllocate,
        source: "WALLET_TO_PORTFOLIO_CREATE",
        portfolio_id: null,
        balance_after: user.wallet_fund,
        created_at: new Date().toISOString(),
      });
    }

    const portfolio = await portfolioRepo.create(
      {
        user_id,
        portfolio_type_id,
        name,
        initial_fund: fundToAllocate,
        available_fund: fundToAllocate,
      },
      client,
    );

    if (user) {
      user.wallet_ledger[user.wallet_ledger.length - 1].portfolio_id = portfolio.id;
      await userRepo.updateWalletState(
        user.id,
        {
          wallet_fund: user.wallet_fund,
          total_fund_added: user.total_fund_added,
          total_fund_withdrawn: user.total_fund_withdrawn,
          wallet_ledger: user.wallet_ledger,
        },
        client,
      );
    }

    return portfolio;
  });
}

async function getUserPortfolios(user_id) {
  const portfolios = await portfolioRepo.listActiveByUser(user_id);
  return attachPortfolioTypes(portfolios);
}

async function getUserPortfolioById({ user_id, portfolio_id }) {
  return portfolioRepo.getActiveById(portfolio_id, user_id);
}

async function getUserPortfolioWithOrders({ user_id, portfolio_id }) {
  const portfolio = await getUserPortfolioById({ user_id, portfolio_id });
  if (!portfolio) return null;

  const orders = await orderRepo.listByUserPortfolio(user_id, portfolio_id);
  return { portfolio, orders };
}

async function getHoldingsByActiveStock({ user_id, active_stock_id }) {
  const portfolios = await portfolioRepo.listActiveByUser(user_id);
  const withTypes = await attachPortfolioTypes(
    portfolios.map((p) => ({
      ...p,
      holdings: Array.isArray(p.holdings) ? p.holdings : [],
    })),
  );

  return withTypes
    .map((p) => {
      const holding = p.holdings.find(
        (h) => String(h.active_stock_id) === String(active_stock_id),
      );

      if (!holding) return null;

      return {
        portfolio_id: p.id,
        portfolio_name: p.name,
        portfolio_type: p.portfolio_type_id,
        holding,
      };
    })
    .filter(Boolean);
}

async function getPortfolioHoldings({ user_id, portfolio_id }) {
  const portfolio = await portfolioRepo.getActiveById(portfolio_id, user_id);
  if (!portfolio) return null;

  const withType = (await attachPortfolioTypes([portfolio]))[0];

  return {
    portfolio_id: withType.id,
    portfolio_name: withType.name,
    portfolio_type: withType.portfolio_type_id,
    holdings: withType.holdings || [],
    available_fund: withType.available_fund,
    initial_fund: withType.initial_fund,
    status: withType.status,
    lock_fund: withType.lock_fund || [],
  };
}

async function getPortfolioHoldingOrders({ user_id, portfolio_id, active_stock_id }) {
  const portfolio = await portfolioRepo.getActiveById(portfolio_id, user_id);
  if (!portfolio) return null;

  const withType = (await attachPortfolioTypes([portfolio]))[0];
  const orders = await orderRepo.listByUserPortfolioStock(
    user_id,
    portfolio_id,
    active_stock_id,
  );

  return {
    portfolio_id: withType.id,
    portfolio_name: withType.name,
    portfolio_type: withType.portfolio_type_id,
    active_stock_id,
    order_count: orders.length,
    orders,
  };
}

async function getPriceMapBySymbol(symbols, fallbackBySymbol) {
  const unique = Array.from(new Set(symbols.filter(Boolean)));
  const entries = await Promise.all(
    unique.map(async (symbol) => {
      try {
        const raw = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${symbol}`);
        if (raw) {
          const data = JSON.parse(raw);
          if (typeof data.ltp === "number") {
            return [symbol, data.ltp];
          }
        }
      } catch (_) {}

      return [symbol, fallbackBySymbol[symbol] ?? 0];
    }),
  );

  return Object.fromEntries(entries);
}

async function buildPriceMapForPortfolios(portfolios) {
  const stockIds = new Set();
  const symbols = new Set();

  portfolios.forEach((p) => {
    (p.holdings || []).forEach((h) => {
      if (h.active_stock_id) stockIds.add(Number(h.active_stock_id));
      if (h.symbol) symbols.add(h.symbol);
    });
  });

  const activeStocks = await activeStockRepo.getByIds(Array.from(stockIds));

  const fallbackBySymbol = {};
  activeStocks.forEach((s) => {
    if (s.symbol) {
      fallbackBySymbol[s.symbol] = toNumber(s.ltp);
      symbols.add(s.symbol);
    }
  });

  return getPriceMapBySymbol(Array.from(symbols), fallbackBySymbol);
}

function calcPortfolioTotals(portfolio, priceBySymbol) {
  const holdings = portfolio.holdings || [];
  const invested = holdings.reduce((sum, h) => sum + toNumber(h.invested_value), 0);
  const current = holdings.reduce((sum, h) => {
    const qty = toNumber(h.quantity);
    const price = priceBySymbol[h.symbol] || 0;
    return sum + qty * price;
  }, 0);

  const available = toNumber(portfolio.available_fund);
  const locked = (portfolio.lock_fund || []).reduce(
    (sum, l) => sum + toNumber(l.locked_amount),
    0,
  );

  return {
    invested_value: invested,
    current_value: current,
    available_fund: available,
    locked_fund: locked,
  };
}

async function getAllPortfoliosSummary({ user_id }) {
  const portfolios = await portfolioRepo.listActiveByUser(user_id);
  const priceBySymbol = await buildPriceMapForPortfolios(portfolios);

  const sellOrders = await orderRepo.listSellExecutedByUser(user_id);
  const realizedByPortfolio = {};
  sellOrders.forEach((o) => {
    const key = String(o.portfolio_id);
    realizedByPortfolio[key] = (realizedByPortfolio[key] || 0) + toNumber(o.realized_pl);
  });

  const totals = {
    invested_value: 0,
    current_value: 0,
    available_fund: 0,
    locked_fund: 0,
    realized_pl: 0,
    unrealized_pl: 0,
    total_pl: 0,
  };

  portfolios.forEach((p) => {
    const t = calcPortfolioTotals(p, priceBySymbol);
    const realized = realizedByPortfolio[String(p.id)] || 0;
    const unrealized = t.current_value - t.invested_value;

    totals.invested_value += t.invested_value;
    totals.current_value += t.current_value;
    totals.available_fund += t.available_fund;
    totals.locked_fund += t.locked_fund;
    totals.realized_pl += realized;
    totals.unrealized_pl += unrealized;
  });

  totals.total_pl = totals.realized_pl + totals.unrealized_pl;
  return totals;
}

async function getPortfolioSummary({ user_id, portfolio_id }) {
  const portfolio = await portfolioRepo.getActiveById(portfolio_id, user_id);
  if (!portfolio) return null;

  const priceBySymbol = await buildPriceMapForPortfolios([portfolio]);
  const totals = calcPortfolioTotals(portfolio, priceBySymbol);

  const realizedPl = await orderRepo.sumRealizedPlByUserPortfolio(user_id, portfolio_id);
  const unrealizedPl = totals.current_value - totals.invested_value;

  return {
    portfolio_id: portfolio.id,
    portfolio_name: portfolio.name,
    invested_value: totals.invested_value,
    current_value: totals.current_value,
    available_fund: totals.available_fund,
    locked_fund: totals.locked_fund,
    realized_pl: realizedPl,
    unrealized_pl: unrealizedPl,
    total_pl: realizedPl + unrealizedPl,
  };
}

async function archiveUserPortfolio({ user_id, portfolio_id }) {
  const archived = await portfolioRepo.archiveByIdAndUser(portfolio_id, user_id);
  if (!archived) {
    throw new Error("Portfolio not found or already archived");
  }
  return true;
}

async function updateAvailableFund({ portfolio_id, amount, session }) {
  const db = session?.client;
  const portfolio = await portfolioRepo.getActiveByIdAnyUser(portfolio_id, db, {
    forUpdate: true,
  });

  if (!portfolio) {
    throw new Error("Active portfolio not found");
  }

  const newFund = toNumber(portfolio.available_fund) + toNumber(amount);
  if (newFund < 0) {
    throw new Error("Insufficient available fund");
  }

  return portfolioRepo.updateFinancialState(
    portfolio_id,
    { available_fund: newFund },
    db,
  );
}

module.exports = {
  createUserPortfolio,
  getUserPortfolios,
  getUserPortfolioById,
  getUserPortfolioWithOrders,
  getHoldingsByActiveStock,
  getPortfolioHoldings,
  getPortfolioHoldingOrders,
  getAllPortfoliosSummary,
  getPortfolioSummary,
  archiveUserPortfolio,
  updateAvailableFund,
};
