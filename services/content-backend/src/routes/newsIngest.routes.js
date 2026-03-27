const express = require("express");
const router = express.Router();
const controller = require("../controllers/newsIngest.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const { fetchAnnouncementsValidator } = require("../validator/newsIngest.validator");

router.post(
  "/fetch",
  authMiddleware,
  fetchAnnouncementsValidator,
  validate,
  controller.fetchAnnouncements,
);

router.get(
  "/list",
  authMiddleware,
  controller.getNewsList,
);

router.get(
  "/categories",
  authMiddleware,
  controller.getNewsCategories,
);

router.get(
  "/news/:id",
  authMiddleware,
  controller.getNewsItem,
);

router.post(
  "/videos",
  authMiddleware,
  controller.createFullVideoRecord,
);

router.get(
  "/videos",
  authMiddleware,
  controller.listFullVideos,
);

module.exports = router;
