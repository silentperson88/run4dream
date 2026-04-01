const express = require("express");
const router = express.Router();

const controller = require("../controllers/taxPlanner.controller");
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const { taxPlannerPlanValidator, taxPlannerPlanIdValidator } = require("../validator/taxPlanner.validator");

router.get("/", authMiddleware, controller.listPlanners);
router.get("/active", authMiddleware, controller.getActivePlanner);
router.post("/", authMiddleware, taxPlannerPlanValidator, validate, controller.savePlanner);
router.get("/:planId", authMiddleware, taxPlannerPlanIdValidator, validate, controller.getPlannerById);
router.patch("/:planId", authMiddleware, taxPlannerPlanIdValidator, taxPlannerPlanValidator, validate, controller.updatePlannerById);

module.exports = router;
