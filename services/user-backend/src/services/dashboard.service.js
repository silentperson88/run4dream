const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/constants/redis.constants");
const userRepo = require("../repositories/users.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const orderRepo = require("../repositories/orders.repository");
const activeStockRepo = require("../repositories/activeStocks.repository");
const { toNumber } = require("../repositories/common");

const DEFAULT_DAYS = 30;

function toDateKey(date) {
  const d = new Date(date);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
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

async function buildPriceMaps({ holdings, sellOrders }) {
  const stockIds = new Set();
  const symbols = new Set();

  holdings.forEach((h) => {
    if (h.active_stock_id) stockIds.add(Number(h.active_stock_id));
    if (h.symbol) symbols.add(h.symbol);
  });

  sellOrders.forEach((o) => {
    if (o.active_stock_id) stockIds.add(Number(o.active_stock_id));
    if (o.symbol) symbols.add(o.symbol);
  });

  const activeStocks = await activeStockRepo.getByIds(Array.from(stockIds));

  const fallbackBySymbol = {};
  activeStocks.forEach((s) => {
    if (s.symbol) {
      fallbackBySymbol[s.symbol] = toNumber(s.ltp);
      symbols.add(s.symbol);
    }
  });

  const priceBySymbol = await getPriceMapBySymbol(Array.from(symbols), fallbackBySymbol);
  return { priceBySymbol, activeStocks };
}

function aggregateHoldings(holdings, priceBySymbol) {
  let totalInvested = 0;
  let totalCurrent = 0;
  const stockIds = new Set();
  const byStock = {};

  holdings.forEach((h) => {
    const invested = toNumber(h.invested_value);
    const qty = toNumber(h.quantity);
    const price = priceBySymbol[h.symbol] || 0;
    const current = qty * price;

    totalInvested += invested;
    totalCurrent += current;

    if (h.active_stock_id) {
      stockIds.add(String(h.active_stock_id));
    }

    const key = String(h.active_stock_id || h.symbol || "unknown");
    if (!byStock[key]) {
      byStock[key] = {
        active_stock_id: h.active_stock_id || null,
        symbol: h.symbol || null,
        total_quantity: 0,
        invested_value: 0,
        current_value: 0,
      };
    }

    byStock[key].total_quantity += qty;
    byStock[key].invested_value += invested;
    byStock[key].current_value += current;
  });

  return {
    totalInvested,
    totalCurrent,
    totalStockTypes: stockIds.size,
    byStock,
  };
}

async function getDashboardData({ user_id, days = DEFAULT_DAYS }) {
  const now = new Date();
  const startDate = new Date(now);
  startDate.setDate(startDate.getDate() - Number(days || DEFAULT_DAYS));

  const user = await userRepo.getById(user_id);
  const portfolios = await portfolioRepo.listActiveByUser(user_id);

  const ordersSummary = {
    total: 0,
    buy: 0,
    sell: 0,
    open: 0,
    partially_filled: 0,
    completed: 0,
    cancelled: 0,
  };

  const orderCounts = await orderRepo.countByStatusAndType(user_id);
  orderCounts.forEach((row) => {
    const status = row.status;
    const type = row.type;
    const count = Number(row.count || 0);

    ordersSummary.total += count;
    if (type === "BUY") ordersSummary.buy += count;
    if (type === "SELL") ordersSummary.sell += count;
    if (status === "OPEN") ordersSummary.open += count;
    if (status === "PARTIALLY_FILLED") ordersSummary.partially_filled += count;
    if (status === "COMPLETED") ordersSummary.completed += count;
    if (status === "CANCELLED") ordersSummary.cancelled += count;
  });

  const holdings = portfolios.flatMap((p) =>
    (p.holdings || []).map((h) => ({ ...h, portfolio_id: p.id, portfolio_name: p.name })),
  );

  const sellOrders = await orderRepo.listSellExecutedByUserFromDate(
    user_id,
    startDate.toISOString(),
  );

  const { priceBySymbol } = await buildPriceMaps({ holdings, sellOrders });
  const totals = aggregateHoldings(holdings, priceBySymbol);

  const totalPL = totals.totalCurrent - totals.totalInvested;

  const allSellOrders = await orderRepo.listSellExecutedByUser(user_id);

  const realizedByPortfolio = {};
  let totalRealized = 0;
  allSellOrders.forEach((o) => {
    const key = String(o.portfolio_id);
    const value = toNumber(o.realized_pl);
    totalRealized += value;
    realizedByPortfolio[key] = (realizedByPortfolio[key] || 0) + value;
  });

  const totalUnrealized = totalPL;

  const portfolioPerformance = portfolios.map((p) => {
    const pHoldings = p.holdings || [];
    const invested = pHoldings.reduce((sum, h) => sum + toNumber(h.invested_value), 0);
    const current = pHoldings.reduce((sum, h) => {
      const qty = toNumber(h.quantity);
      const price = priceBySymbol[h.symbol] || 0;
      return sum + qty * price;
    }, 0);

    const unrealized = current - invested;
    const realized = realizedByPortfolio[String(p.id)] || 0;
    const lockedFund = (p.lock_fund || []).reduce(
      (sum, l) => sum + toNumber(l.locked_amount),
      0,
    );

    return {
      portfolio_id: p.id,
      portfolio_name: p.name,
      invested_value: invested,
      current_value: current,
      available_fund: toNumber(p.available_fund),
      locked_fund: lockedFund,
      total_fund: invested + toNumber(p.available_fund) + lockedFund,
      unrealized_pl: unrealized,
      realized_pl: realized,
      pl: unrealized + realized,
    };
  });

  const topInvestedStocks = Object.values(totals.byStock)
    .map((s) => ({ ...s, pl: s.current_value - s.invested_value }))
    .sort((a, b) => b.invested_value - a.invested_value)
    .slice(0, 5);

  const topSymbols = topInvestedStocks.map((s) => s.symbol).filter(Boolean);
  if (topSymbols.length) {
    const names = await activeStockRepo.getBySymbols(topSymbols);
    const nameBySymbol = {};
    names.forEach((r) => {
      nameBySymbol[r.symbol] = r.name || r.symbol;
    });

    topInvestedStocks.forEach((s) => {
      if (s.symbol && nameBySymbol[s.symbol]) {
        s.symbol = nameBySymbol[s.symbol];
      }
    });
  }

  const buyOrders = await orderRepo.listBuyExecutedByUserFromDate(
    user_id,
    startDate.toISOString(),
  );

  const investedByDate = {};
  buyOrders.forEach((o) => {
    const invested =
      toNumber(o.executed_quantity) * toNumber(o.avg_execution_price || o.order_price);
    const key = toDateKey(o.createdAt || o.created_at);
    investedByDate[key] = (investedByDate[key] || 0) + invested;
  });

  const newInvestments = Object.keys(investedByDate)
    .sort()
    .map((date) => ({ date, invested_value: investedByDate[date] }));

  const salesByPortfolio = portfolios.map((p) => {
    const pHoldings = p.holdings || [];
    const invested = pHoldings.reduce((sum, h) => sum + toNumber(h.invested_value), 0);
    const current = pHoldings.reduce((sum, h) => {
      const qty = toNumber(h.quantity);
      const price = priceBySymbol[h.symbol] || 0;
      return sum + qty * price;
    }, 0);

    const types = new Set(
      pHoldings.map((h) => String(h.active_stock_id || "")).filter(Boolean),
    );

    const unrealized = current - invested;
    const realized = realizedByPortfolio[String(p.id)] || 0;

    return {
      portfolio_id: p.id,
      portfolio_name: p.name,
      total_invested: invested,
      available_fund: toNumber(p.available_fund),
      unrealized_pl: unrealized,
      realized_pl: realized,
      pl: unrealized + realized,
      total_stock_types: types.size,
    };
  });

  const distribution = portfolios.map((p) => {
    const pHoldings = p.holdings || [];
    const invested = pHoldings.reduce((sum, h) => sum + toNumber(h.invested_value), 0);
    const current = pHoldings.reduce((sum, h) => {
      const qty = toNumber(h.quantity);
      const price = priceBySymbol[h.symbol] || 0;
      return sum + qty * price;
    }, 0);

    return {
      portfolio_id: p.id,
      portfolio_name: p.name,
      invested_value: invested,
      available_fund: toNumber(p.available_fund),
      total_value: invested + toNumber(p.available_fund),
      current_value: current,
    };
  });

  const soldPerformanceMap = {};
  sellOrders.forEach((o) => {
    const key = String(o.active_stock_id || o.symbol || "unknown");
    if (!soldPerformanceMap[key]) {
      soldPerformanceMap[key] = {
        active_stock_id: o.active_stock_id || null,
        symbol: o.symbol || null,
        total_sold_qty: 0,
        avg_sell_price: 0,
        sell_value: 0,
        current_value: 0,
        performance: 0,
      };
    }

    const qty = toNumber(o.executed_quantity);
    const price = toNumber(o.avg_execution_price || o.order_price);
    soldPerformanceMap[key].total_sold_qty += qty;
    soldPerformanceMap[key].sell_value += qty * price;
  });

  Object.values(soldPerformanceMap).forEach((s) => {
    s.avg_sell_price = s.total_sold_qty > 0 ? s.sell_value / s.total_sold_qty : 0;
    const price = priceBySymbol[s.symbol] || 0;
    s.current_value = s.total_sold_qty * price;
    s.performance = s.current_value - s.sell_value;
  });

  const soldPerformance = Object.values(soldPerformanceMap);

  const lastTrades = await orderRepo.listLatestByUser(user_id, 20);

  const totalAvailableFund = portfolios.reduce(
    (sum, p) => sum + toNumber(p.available_fund),
    0,
  );

  const totalLockedFund = portfolios.reduce((sum, p) => {
    const locked = (p.lock_fund || []).reduce((s, l) => s + toNumber(l.locked_amount), 0);
    return sum + locked;
  }, 0);

  return {
    total_pl: totalPL,
    stats: {
      total_invested: totals.totalInvested,
      total_current_value: totals.totalCurrent,
      total_stock_types: totals.totalStockTypes,
      revenue: totalPL + totalRealized,
      realized_pl: totalRealized,
      unrealized_pl: totalUnrealized,
      total_orders: ordersSummary.total,
      open_orders: ordersSummary.open,
      partially_filled_orders: ordersSummary.partially_filled,
      completed_orders: ordersSummary.completed,
      cancelled_orders: ordersSummary.cancelled,
    },
    account_totals: {
      invested_value: totals.totalInvested,
      wallet_fund: toNumber(user?.wallet_fund),
      total_fund_added: toNumber(user?.total_fund_added),
      total_fund_withdrawn: toNumber(user?.total_fund_withdrawn),
      available_fund: totalAvailableFund,
      locked_fund: totalLockedFund,
      current_value: totals.totalCurrent,
      realized_pl: totalRealized,
      unrealized_pl: totalUnrealized,
      total_pl: totalRealized + totalUnrealized,
      total_wallet_value:
        toNumber(user?.wallet_fund) +
        totalAvailableFund +
        totalLockedFund +
        totals.totalCurrent,
    },
    orders_summary: ordersSummary,
    portfolio_performance: portfolioPerformance,
    total_earnings: topInvestedStocks,
    new_investments: {
      days: Number(days),
      total_invested: newInvestments.reduce(
        (sum, i) => sum + toNumber(i.invested_value),
        0,
      ),
      series: newInvestments,
    },
    sales_by_portfolio: salesByPortfolio,
    amount_distribution: distribution,
    recent_sell_performance: {
      days: Number(days),
      items: soldPerformance,
    },
    last_trades: lastTrades,
  };
}

module.exports = { getDashboardData };
