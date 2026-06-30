const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/constants/redis.constants");
const { MESSAGES } = require("../utils/constants/response.constants");
const { withTransaction } = require("../repositories/tx");
const stockRepo = require("../repositories/activeStocks.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const orderRepo = require("../repositories/orders.repository");
const portfolioTypeRepo = require("../repositories/portfolioTypes.repository");
const { toNumber } = require("../repositories/common");

const calculateDiffPercent = (orderPrice, ltp) =>
  (Math.abs(orderPrice - ltp) / ltp) * 100;

const getTodayInIst = () =>
  new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());

const isBacktestingPortfolio = async (portfolio, client) => {
  if (String(portfolio?.meta?.mode || "").toUpperCase() === "BACKTEST") return true;
  const type = await portfolioTypeRepo.getActiveById(portfolio.portfolio_type_id, client);
  return String(type?.code || "").toUpperCase() === "BACKTESTING";
};

const placeOrderService = async (payload, userId) =>
  withTransaction(async (client) => {
    const {
      portfolio_id,
      active_stock_id,
      stock_symbol,
      type,
      order_type,
      quantity,
      price,
      simulated_trade_date,
    } = payload;

    const stock = await stockRepo.getById(active_stock_id, client);
    if (!stock || !stock.is_active) {
      throw new Error(MESSAGES.ORDER.INVALID_STOCK);
    }

    const portfolio = await portfolioRepo.getActiveById(
      portfolio_id,
      userId,
      client,
      { forUpdate: true },
    );

    if (!portfolio) {
      throw new Error(MESSAGES.ORDER.ORDER_REJECTED);
    }

    const backtesting = await isBacktestingPortfolio(portfolio, client);
    let stockData = null;
    let executionPrice = 0;

    if (backtesting) {
      if (stock.symbol !== stock_symbol) {
        throw new Error(MESSAGES.ORDER.INVALID_STOCK);
      }
      const baseAsOfDate = String(portfolio?.meta?.as_of_date || "");
      const effectiveTradeDate = String(simulated_trade_date || baseAsOfDate);
      const todayInIst = getTodayInIst();

      if (!baseAsOfDate) {
        throw new Error("Backtesting portfolio is missing as-of date");
      }

      if (effectiveTradeDate < baseAsOfDate || effectiveTradeDate > todayInIst) {
        throw new Error(
          `Backtesting trade date must be between ${baseAsOfDate} and ${todayInIst}`,
        );
      }

      executionPrice = order_type === "MARKET" ? toNumber(price) : toNumber(price);
      if (!Number.isFinite(executionPrice) || executionPrice <= 0) {
        throw new Error("Backtesting order requires a valid historical price");
      }

      payload.simulated_trade_date = effectiveTradeDate;
    } else {
      const redisData = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`);
      if (!redisData) {
        throw new Error(MESSAGES.COMMON.CACHE_ERROR);
      }

      stockData = JSON.parse(redisData);
      if (stock.symbol !== stock_symbol || stockData.symbol !== stock_symbol) {
        throw new Error(MESSAGES.ORDER.INVALID_STOCK);
      }

      executionPrice =
        order_type === "MARKET" ? toNumber(stockData.ltp) : toNumber(price);
      if (!Number.isFinite(executionPrice) || executionPrice <= 0) {
        throw new Error(MESSAGES.ORDER.ORDER_REJECTED);
      }

      if (
        executionPrice < toNumber(stock.lower_circuit) ||
        executionPrice > toNumber(stock.upper_circuit)
      ) {
        throw new Error(MESSAGES.ORDER.ORDER_REJECTED_OUT_OF_CIRCUIT);
      }
    }

    const holdings = [...portfolio.holdings];
    const lockFund = [...portfolio.lock_fund];
    let availableFund = toNumber(portfolio.available_fund);

    const qty = Number(quantity);
    const orderValue = executionPrice * qty;
    const effectiveUpdateTimestamp = backtesting
      ? `${payload.simulated_trade_date || portfolio?.meta?.as_of_date || getTodayInIst()}T00:00:00.000Z`
      : new Date().toISOString();

    if (type === "BUY" && availableFund < orderValue) {
      throw new Error(MESSAGES.ORDER.INSUFFICIENT_FUNDS);
    }

    if (type === "SELL") {
      const holding = holdings.find(
        (h) => String(h.active_stock_id) === String(active_stock_id),
      );

      if (
        !holding ||
        toNumber(holding.quantity) - toNumber(holding.locked_sell_quantity) < qty
      ) {
        throw new Error(MESSAGES.ORDER.INSUFFICIENT_QTY);
      }
    }

    let status = "OPEN";
    let executedQuantity = 0;
    const executions = [];
    let realizedPl = 0;

    if (backtesting) {
      status = "COMPLETED";
      executedQuantity = qty;
      executions.push({ quantity: qty, price: executionPrice });
    } else if (order_type === "MARKET") {
      status = "COMPLETED";
      executedQuantity = qty;
      executions.push({ quantity: qty, price: executionPrice });
    } else {
      const diffPercent = calculateDiffPercent(executionPrice, stockData.ltp);

      if (diffPercent <= MESSAGES.ORDER.FULL_DIFF_PERCENT) {
        status = "COMPLETED";
        executedQuantity = qty;
        executions.push({ quantity: qty, price: executionPrice });
      } else if (
        diffPercent >= MESSAGES.ORDER.PARTIAL_MIN_PERCENT &&
        diffPercent <= MESSAGES.ORDER.PARTIAL_MAX_PERCENT
      ) {
        status = "PARTIALLY_FILLED";
        const minQty = Math.ceil(qty * MESSAGES.ORDER.PARTIAL_MIN_QTY_RATIO);
        const maxQty = Math.ceil(qty * MESSAGES.ORDER.PARTIAL_MAX_QTY_RATIO);
        executedQuantity =
          Math.floor(Math.random() * (maxQty - minQty + 1)) + minQty;

        executions.push({ quantity: executedQuantity, price: executionPrice });
      }
    }

    const remainingQuantity = qty - executedQuantity;

    if (type === "BUY") {
      const lockedAmount = remainingQuantity * executionPrice;
      availableFund -= orderValue;

      if (lockedAmount > 0) {
        lockFund.push({ order_id: null, locked_amount: lockedAmount });
      }

      if (executedQuantity > 0) {
        let holding = holdings.find(
          (h) => String(h.active_stock_id) === String(active_stock_id),
        );

        if (!holding) {
          holdings.push({
            active_stock_id: Number(active_stock_id),
            symbol: stock.symbol,
            quantity: executedQuantity,
            locked_sell_quantity: 0,
            avg_buy_price: executionPrice,
            invested_value: executedQuantity * executionPrice,
            last_updated_at: effectiveUpdateTimestamp,
          });
        } else {
          const totalQty = toNumber(holding.quantity) + executedQuantity;
          holding.avg_buy_price =
            (toNumber(holding.quantity) * toNumber(holding.avg_buy_price) +
              executedQuantity * executionPrice) /
            totalQty;
          holding.quantity = totalQty;
          holding.invested_value =
            toNumber(holding.invested_value) + executedQuantity * executionPrice;
          holding.last_updated_at = effectiveUpdateTimestamp;
        }
      }
    }

    if (type === "SELL") {
      const holding = holdings.find(
        (h) => String(h.active_stock_id) === String(active_stock_id),
      );

      holding.locked_sell_quantity = toNumber(holding.locked_sell_quantity) + qty;

      if (executedQuantity > 0) {
        holding.quantity = toNumber(holding.quantity) - executedQuantity;
        holding.locked_sell_quantity =
          toNumber(holding.locked_sell_quantity) - executedQuantity;

        const reduceValue = executedQuantity * toNumber(holding.avg_buy_price);
        holding.invested_value = Math.max(
          toNumber(holding.invested_value) - reduceValue,
          0,
        );

        realizedPl =
          (executionPrice - toNumber(holding.avg_buy_price)) * executedQuantity;

        availableFund += executedQuantity * executionPrice;
        holding.last_updated_at = effectiveUpdateTimestamp;
      }

      if (toNumber(holding.quantity) === 0) {
        const nextHoldings = holdings.filter(
          (h) => String(h.active_stock_id) !== String(active_stock_id),
        );
        holdings.length = 0;
        holdings.push(...nextHoldings);
      }
    }

    const order = await orderRepo.create(
      {
        user_id: userId,
        portfolio_id,
        active_stock_id,
        symbol: stock.symbol,
        exchange: stock.exchange,
        type,
        order_type,
        order_price: executionPrice,
        order_quantity: qty,
        executed_quantity: executedQuantity,
        remaining_quantity: remainingQuantity,
        avg_execution_price: executedQuantity > 0 ? executionPrice : 0,
        status,
        executions,
        realized_pl: type === "SELL" ? realizedPl : 0,
        max_partial_executions: 5,
        sell_allocation: [],
        simulation_mode: backtesting ? "BACKTEST" : "LIVE",
        simulated_trade_date: backtesting ? payload.simulated_trade_date || null : null,
      },
      client,
    );

    if (type === "BUY" && remainingQuantity > 0) {
      lockFund[lockFund.length - 1].order_id = order.id;
    }

    await portfolioRepo.updateFinancialState(
      portfolio_id,
      {
        available_fund: availableFund,
        holdings,
        lock_fund: lockFund,
      },
      client,
    );

    return {
      order,
      ltp: backtesting ? executionPrice : stockData.ltp,
      simulation_mode: backtesting ? "BACKTEST" : "LIVE",
    };
  });

const listOpenOrdersByUser = (userId) => orderRepo.listOpenByUser(userId);

const listOpenOrdersByPortfolio = (userId, portfolioId) =>
  orderRepo.listOpenByPortfolio(userId, portfolioId);

module.exports = {
  placeOrderService,
  listOpenOrdersByUser,
  listOpenOrdersByPortfolio,
};
