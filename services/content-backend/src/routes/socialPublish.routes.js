const express = require("express");
const router = express.Router();
const { body, param } = require("express-validator");
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const controller = require("../controllers/socialPublish.controller");

const publishValidator = [
  param("platform").isString().withMessage("platform is required"),
  body("title").optional().isString().withMessage("title must be a string"),
  body("caption").optional().isString().withMessage("caption must be a string"),
  body("imageUrl").optional().isString().withMessage("imageUrl must be a string"),
  body("link").optional().isString().withMessage("link must be a string"),
  body("rssItemId").optional().isInt().withMessage("rssItemId must be an integer"),
];

const prepValidator = [
  param("platform").isString().withMessage("platform is required"),
  body("title").optional().isString().withMessage("title must be a string"),
  body("caption").optional().isString().withMessage("caption must be a string"),
  body("rssItemId").optional().isInt().withMessage("rssItemId must be an integer"),
  body("templateType").optional().isString().withMessage("templateType must be a string"),
  body("templateProps").optional().isObject().withMessage("templateProps must be an object"),
];

const scheduleValidator = [
  param("platform").isString().withMessage("platform is required"),
  body("rssItemId").isInt().withMessage("rssItemId is required"),
  body("scheduledAt").isString().withMessage("scheduledAt is required"),
  body("title").optional().isString().withMessage("title must be a string"),
  body("caption").optional().isString().withMessage("caption must be a string"),
  body("templateType").optional().isString().withMessage("templateType must be a string"),
  body("templateProps").optional().isObject().withMessage("templateProps must be an object"),
  body("renderJobId").optional().isString().withMessage("renderJobId must be a string"),
];

router.post("/social-accounts/:platform/publish", authMiddleware, publishValidator, validate, controller.publishSocialPost);
router.post("/social-accounts/:platform/prepare", authMiddleware, prepValidator, validate, controller.prepareSocialPost);
router.post("/social-accounts/:platform/schedule", authMiddleware, scheduleValidator, validate, controller.scheduleSocialPost);
router.get("/social-accounts/:platform/prepare/:jobId", authMiddleware, controller.getPrepareSocialPostStatus);

module.exports = router;
