const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/constants/redis.constants");
const { MESSAGES } = require("../utils/constants/response.constants");
const { withTransaction } = require("../repositories/tx");
const stockRepo = require("../repositories/activeStocks.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const orderRepo = require("../repositories/orders.repository");
const { toNumber } = require("../repositories/common");

const calculateDiffPercent = (orderPrice, ltp) =>
  (Math.abs(orderPrice - ltp) / ltp) * 100;

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
    } = payload;

    const stock = await stockRepo.getById(active_stock_id, client);
    if (!stock || !stock.is_active) {
      throw new Error(MESSAGES.ORDER.INVALID_STOCK);
    }

    const redisData = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${stock.symbol}`);
    if (!redisData) {
      throw new Error(MESSAGES.COMMON.CACHE_ERROR);
    }

    const stockData = JSON.parse(redisData);
    if (stock.symbol !== stock_symbol || stockData.symbol !== stock_symbol) {
      throw new Error(MESSAGES.ORDER.INVALID_STOCK);
    }

    const executionPrice =
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

    const portfolio = await portfolioRepo.getActiveById(
      portfolio_id,
      userId,
      client,
      { forUpdate: true },
    );

    if (!portfolio) {
      throw new Error(MESSAGES.ORDER.ORDER_REJECTED);
    }

    const holdings = [...portfolio.holdings];
    const lockFund = [...portfolio.lock_fund];
    let availableFund = toNumber(portfolio.available_fund);

    const qty = Number(quantity);
    const orderValue = executionPrice * qty;

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

    if (order_type === "MARKET") {
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

    return { order, lll: stockData.ltp };
  });

const listOpenOrdersByUser = (userId) => orderRepo.listOpenByUser(userId);

const listOpenOrdersByPortfolio = (userId, portfolioId) =>
  orderRepo.listOpenByPortfolio(userId, portfolioId);

module.exports = {
  placeOrderService,
  listOpenOrdersByUser,
  listOpenOrdersByPortfolio,
};
