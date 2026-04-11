const express = require("express");
const router = express.Router();
const { body, param } = require("express-validator");
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const controller = require("../controllers/socialAccounts.controller");

const upsertValidator = [
  param("platform").isString().withMessage("platform is required"),
  body("accountLabel").optional().isString().withMessage("accountLabel must be a string"),
  body("isConnected").optional().isBoolean().withMessage("isConnected must be a boolean"),
  body("notes").optional().isString().withMessage("notes must be a string"),
  body("connectionData").optional().isObject().withMessage("connectionData must be an object"),
];

router.get("/social-accounts/schema", authMiddleware, controller.getSocialAccountSchema);
router.get("/social-accounts", authMiddleware, controller.getSocialAccounts);
router.get("/social-accounts/:platform", authMiddleware, controller.getSocialAccount);
router.put("/social-accounts/:platform", authMiddleware, upsertValidator, validate, controller.upsertSocialAccount);
router.delete("/social-accounts/:platform", authMiddleware, controller.deleteSocialAccount);

module.exports = router;
