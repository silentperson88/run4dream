const cron = require("node-cron");
const { pool } = require("../config/db");
const redis = require("../config/redis.config");
const { REDIS_KEYS } = require("../utils/constants/redis.constants");
const { MESSAGES } = require("../utils/constants/response.constants");
const { isMarketClosed } = require("../utils/method.utils");
const orderRepo = require("../repositories/orders.repository");
const portfolioRepo = require("../repositories/userPortfolios.repository");
const { toNumber } = require("../repositories/common");

const sameOrder = (lock, orderId) => String(lock?.order_id) === String(orderId);
const sameStock = (holding, stockId) => String(holding?.active_stock_id) === String(stockId);

cron.schedule("*/1 * * * *", async () => {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const marketClosed = await isMarketClosed();

    const orders = await orderRepo.listOpenForExecution(client);

    for (const order of orders) {
      const portfolio = await portfolioRepo.getActiveByIdAnyUser(
        order.portfolio_id,
        client,
        { forUpdate: true },
      );

      if (!portfolio) continue;

      const holdings = Array.isArray(portfolio.holdings) ? portfolio.holdings : [];
      const lockFund = Array.isArray(portfolio.lock_fund) ? portfolio.lock_fund : [];
      const executions = Array.isArray(order.executions) ? order.executions : [];
      let availableFund = toNumber(portfolio.available_fund);
      let orderStatus = order.status;
      let executedQuantity = Number(order.executed_quantity || 0);
      let remainingQuantity = Number(order.remaining_quantity || 0);
      let realizedPl = toNumber(order.realized_pl);

      if (marketClosed) {
        if (order.type === "BUY") {
          const lock = lockFund.find((l) => sameOrder(l, order.id));
          if (lock) {
            availableFund += toNumber(lock.locked_amount);
            const filtered = lockFund.filter((l) => !sameOrder(l, order.id));
            lockFund.length = 0;
            lockFund.push(...filtered);
          }
        }

        if (order.type === "SELL") {
          const holding = holdings.find((h) => sameStock(h, order.active_stock_id));
          if (holding) {
            holding.locked_sell_quantity = Math.max(
              toNumber(holding.locked_sell_quantity) - remainingQuantity,
              0,
            );
          }
        }

        orderStatus = "CANCELLED";

        await orderRepo.updateStatus(order.id, orderStatus, client);
        await portfolioRepo.updateFinancialState(
          portfolio.id,
          {
            available_fund: availableFund,
            holdings,
            lock_fund: lockFund,
          },
          client,
        );

        continue;
      }

      const stockDataRaw = await redis.get(`${REDIS_KEYS.STOCK_SNAPSHOT}${order.symbol}`);
      if (!stockDataRaw) continue;

      const { ltp } = JSON.parse(stockDataRaw);
      const ltpValue = toNumber(ltp);
      if (ltpValue <= 0) continue;

      const diff = (Math.abs(toNumber(order.order_price) - ltpValue) / ltpValue) * 100;
      if (diff > MESSAGES.ORDER.PARTIAL_MAX_PERCENT) continue;

      let executeQty;
      const remaining = remainingQuantity;

      if (order.max_partial_executions && executions.length >= Number(order.max_partial_executions)) {
        executeQty = remaining;
      } else {
        const min = Math.ceil(remaining * MESSAGES.ORDER.PARTIAL_MIN_QTY_RATIO);
        const max = Math.ceil(remaining * MESSAGES.ORDER.PARTIAL_MAX_QTY_RATIO);
        executeQty = Math.floor(Math.random() * (max - min + 1)) + min;
      }

      executeQty = Math.min(executeQty, remaining);
      if (executeQty <= 0) continue;

      if (order.type === "BUY") {
        const cost = executeQty * toNumber(order.order_price);
        const lock = lockFund.find((l) => sameOrder(l, order.id));
        if (!lock || toNumber(lock.locked_amount) < cost) continue;

        lock.locked_amount = toNumber(lock.locked_amount) - cost;

        let holding = holdings.find((h) => sameStock(h, order.active_stock_id));
        if (!holding) {
          holdings.push({
            active_stock_id: Number(order.active_stock_id),
            symbol: order.symbol,
            quantity: executeQty,
            avg_buy_price: toNumber(order.order_price),
            locked_sell_quantity: 0,
            invested_value: cost,
          });
        } else {
          const total =
            toNumber(holding.quantity) * toNumber(holding.avg_buy_price) + cost;
          holding.quantity = toNumber(holding.quantity) + executeQty;
          holding.avg_buy_price = total / toNumber(holding.quantity);
          holding.invested_value = toNumber(holding.invested_value) + cost;
        }
      }

      if (order.type === "SELL") {
        const holding = holdings.find((h) => sameStock(h, order.active_stock_id));
        if (!holding) continue;

        executeQty = Math.min(executeQty, toNumber(holding.locked_sell_quantity));
        if (executeQty <= 0) continue;

        holding.quantity = toNumber(holding.quantity) - executeQty;
        holding.locked_sell_quantity =
          toNumber(holding.locked_sell_quantity) - executeQty;

        const reduceValue = executeQty * toNumber(holding.avg_buy_price);
        holding.invested_value = Math.max(
          toNumber(holding.invested_value) - reduceValue,
          0,
        );

        realizedPl +=
          (toNumber(order.order_price) - toNumber(holding.avg_buy_price)) * executeQty;

        availableFund += executeQty * toNumber(order.order_price);

        if (toNumber(holding.quantity) === 0) {
          const filtered = holdings.filter(
            (h) => !sameStock(h, holding.active_stock_id),
          );
          holdings.length = 0;
          holdings.push(...filtered);
        }
      }

      executedQuantity += executeQty;
      remainingQuantity -= executeQty;
      executions.push({ quantity: executeQty, price: toNumber(order.order_price) });

      if (remainingQuantity === 0) {
        orderStatus = "COMPLETED";
        const filtered = lockFund.filter((l) => !sameOrder(l, order.id));
        lockFund.length = 0;
        lockFund.push(...filtered);
      } else {
        orderStatus = "PARTIALLY_FILLED";
      }

      await orderRepo.updateExecutionState(
        order.id,
        {
          executed_quantity: executedQuantity,
          remaining_quantity: remainingQuantity,
          executions,
          realized_pl: realizedPl,
          status: orderStatus,
        },
        client,
      );

      await portfolioRepo.updateFinancialState(
        portfolio.id,
        {
          available_fund: availableFund,
          holdings,
          lock_fund: lockFund,
        },
        client,
      );
    }

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("Order cron failed", e);
  } finally {
    client.release();
  }
});

module.exports = {};
