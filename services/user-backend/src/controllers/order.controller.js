const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");
const {
  placeOrderService,
  listOpenOrdersByUser,
  listOpenOrdersByPortfolio,
} = require("../services/order.service");

const placeOrder = async (req, res) => {
  try {
    const order = await placeOrderService(req.body, req.user.id);
    return response(res, 201, MESSAGES.ORDER.ORDER_PLACED, order);
  } catch (err) {
    const msg = err.message || MESSAGES.ORDER.ORDER_FAILED;
    return response(res, 400, msg);
  }
};

const getOpenOrders = async (req, res) => {
  try {
    const data = await listOpenOrdersByUser(req.user.id);
    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
};

const getOpenOrdersByPortfolio = async (req, res) => {
  try {
    const data = await listOpenOrdersByPortfolio(
      req.user.id,
      req.params.portfolioId,
    );
    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
};

module.exports = { placeOrder, getOpenOrders, getOpenOrdersByPortfolio };
