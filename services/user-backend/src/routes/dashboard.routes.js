const express = require("express");
const router = express.Router();

const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const { getDashboard } = require("../controllers/dashboard.controller");
const { dashboardQueryValidator } = require("../validator/dashboard.validator");

router.get(
  "/",
  authMiddleware,
  dashboardQueryValidator,
  validate,
  getDashboard,
);

module.exports = router;
