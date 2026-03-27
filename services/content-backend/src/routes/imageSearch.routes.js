const express = require("express");
const router = express.Router();
const controller = require("../controllers/imageSearch.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const { imageSearchValidator } = require("../validator/imageSearch.validator");

router.get(
  "/image-search",
  authMiddleware,
  imageSearchValidator,
  validate,
  controller.search,
);

module.exports = router;
