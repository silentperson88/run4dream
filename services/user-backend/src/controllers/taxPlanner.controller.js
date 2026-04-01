const taxPlannerService = require("../services/taxPlanner.service");
const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");

async function savePlanner(req, res) {
  try {
    const data = await taxPlannerService.savePlanner({
      user_id: req.user.id,
      ...req.body,
    });

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

async function listPlanners(req, res) {
  try {
    const data = await taxPlannerService.listPlanners({ user_id: req.user.id });
    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

async function getActivePlanner(req, res) {
  try {
    const data = await taxPlannerService.getActivePlanner({ user_id: req.user.id });
    return response(res, 200, MESSAGES.COMMON.SUCCESS, data || null);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

async function getPlannerById(req, res) {
  try {
    const data = await taxPlannerService.getPlannerById({
      user_id: req.user.id,
      plan_id: req.params.planId,
    });

    if (!data) {
      return response(res, 404, "Tax planner not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 500, err.message);
  }
}

async function updatePlannerById(req, res) {
  try {
    const data = await taxPlannerService.updatePlannerById({
      user_id: req.user.id,
      plan_id: req.params.planId,
      ...req.body,
    });

    if (!data) {
      return response(res, 404, "Tax planner not found");
    }

    return response(res, 200, MESSAGES.COMMON.SUCCESS, data);
  } catch (err) {
    return response(res, 400, err.message);
  }
}

module.exports = {
  savePlanner,
  listPlanners,
  getActivePlanner,
  getPlannerById,
  updatePlannerById,
};
