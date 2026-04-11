const express = require("express");
const router = express.Router();
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const { query, body } = require("express-validator");
const controller = require("../controllers/rss.controller");

const rssValidator = [
  query("source").isIn(["toi", "ht"]).withMessage("source must be toi or ht"),
];

const listValidator = [
  query("limit").optional().isInt({ min: 1, max: 200 }).withMessage("limit must be 1-200"),
  query("offset").optional().isInt({ min: 0, max: 100000 }).withMessage("offset must be 0-100000"),
  query("fromDate").optional().isISO8601().withMessage("fromDate must be YYYY-MM-DD"),
  query("toDate").optional().isISO8601().withMessage("toDate must be YYYY-MM-DD"),
];

const saveValidator = [
  body("link").isString().withMessage("link is required"),
  body("title").isString().withMessage("title is required"),
  body("source").isString().withMessage("source is required"),
  body("pubDate").optional().isString().withMessage("pubDate must be a string"),
];
const updateDraftValidator = [
  body("images").optional().isArray().withMessage("images must be an array"),
  body("templateMusicSelection").optional().isObject().withMessage("templateMusicSelection must be an object"),
];

const processOneValidator = [
  body("link").isString().withMessage("link is required"),
];
const fetchBodyValidator = [
  body("link").isString().withMessage("link is required"),
];
const improveTemplateValidator = [
  body("templateType")
    .isIn(["templateOne", "templateTwo", "templateThree"])
    .withMessage("templateType must be templateOne, templateTwo, or templateThree"),
  body("cleanedText")
    .optional()
    .isString()
    .withMessage("cleanedText must be a string"),
  body("title")
    .optional()
    .isString()
    .withMessage("title must be a string"),
];

router.get("/rss", authMiddleware, rssValidator, validate, controller.getRss);
router.get("/rss/items", authMiddleware, listValidator, validate, controller.listRss);
router.get("/rss/progress", authMiddleware, validate, controller.progressRss);

router.post("/rss/save", authMiddleware, saveValidator, validate, controller.saveRss);
router.put("/rss/:id/draft", authMiddleware, updateDraftValidator, validate, controller.updateRssDraft);
router.put("/rss/items/:id/draft", authMiddleware, updateDraftValidator, validate, controller.updateRssDraft);
router.post("/rss/process-one", authMiddleware, processOneValidator, validate, controller.processOne);
router.post("/rss/article-body", authMiddleware, fetchBodyValidator, validate, controller.fetchArticleBody);
router.post("/rss/link-video", authMiddleware, fetchBodyValidator, validate, controller.linkNewsContentVideo);
router.post("/rss/improve-template", authMiddleware, improveTemplateValidator, validate, controller.improveSocialTemplate);
router.get("/rss/template-prompts", authMiddleware, controller.getTemplatePrompts);

module.exports = router;
