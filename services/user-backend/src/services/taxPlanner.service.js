const taxPlannerRepo = require("../repositories/taxPlanner.repository");

function normalizePlannerInput(payload = {}) {
  return {
    name: String(payload.name || "Default").trim() || "Default",
    buy_lots: Array.isArray(payload.buyLots) ? payload.buyLots : Array.isArray(payload.buy_lots) ? payload.buy_lots : [],
    sell_trades: Array.isArray(payload.sellTrades) ? payload.sellTrades : Array.isArray(payload.sell_trades) ? payload.sell_trades : [],
    settings: payload.settings && typeof payload.settings === "object" ? payload.settings : {},
  };
}

async function savePlanner({ user_id, ...payload }) {
  const normalized = normalizePlannerInput(payload);
  return taxPlannerRepo.upsertByName({ user_id, ...normalized });
}

async function listPlanners({ user_id }) {
  return taxPlannerRepo.listByUser(user_id);
}

async function getActivePlanner({ user_id }) {
  return taxPlannerRepo.getLatestByUser(user_id);
}

async function getPlannerById({ user_id, plan_id }) {
  return taxPlannerRepo.getById(plan_id, user_id);
}

async function updatePlannerById({ user_id, plan_id, ...payload }) {
  const normalized = normalizePlannerInput(payload);
  return taxPlannerRepo.updateById({ plan_id, user_id, ...normalized });
}

module.exports = {
  savePlanner,
  listPlanners,
  getActivePlanner,
  getPlannerById,
  updatePlannerById,
};
