// routes/authRoutes.js
const express = require("express");
const router = express.Router();

const portfolioTypeController = require("../controllers/portfolioType.controller");
const validateRequest = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");

router.get(
  "/",
  authMiddleware,
  validateRequest,
  portfolioTypeController.getPortFoliosTypeList,
);

module.exports = router;
