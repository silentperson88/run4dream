const express = require("express");
const router = express.Router();
const { authMiddleware } = require("../middlewares/auth.middleware");
const validate = require("../middlewares/validateRequest.middleware");
const { body, query, param } = require("express-validator");
const controller = require("../controllers/musicLibrary.controller");

const categoryCreateValidator = [
  body("categoryName").isString().withMessage("categoryName is required"),
];

const trackListValidator = [
  query("search").optional().isString().withMessage("search must be a string"),
  query("categoryId").optional().isInt({ min: 1 }).withMessage("categoryId must be a positive integer"),
];

const trackUploadValidator = [
  body("fileName").isString().withMessage("fileName is required"),
  body("dataUrl").optional().isString().withMessage("dataUrl must be a string"),
  body("sourceUrl").optional().isString().withMessage("sourceUrl must be a string"),
  body("title").optional().isString().withMessage("title must be a string"),
  body("categoryIds").optional().isArray().withMessage("categoryIds must be an array"),
];

const trackUpdateValidator = [
  param("id").isInt({ min: 1 }).withMessage("id must be a positive integer"),
  body("title").optional().isString().withMessage("title must be a string"),
  body("categoryIds").optional().isArray().withMessage("categoryIds must be an array"),
];

router.get("/music-library/categories", authMiddleware, controller.getMusicCategories);
router.post("/music-library/categories", authMiddleware, categoryCreateValidator, validate, controller.createMusicCategory);
router.get("/music-library/tracks", authMiddleware, trackListValidator, validate, controller.getMusicTracks);
router.post("/music-library/tracks/upload", authMiddleware, trackUploadValidator, validate, controller.uploadMusicTrack);
router.put("/music-library/tracks/:id", authMiddleware, trackUpdateValidator, validate, controller.updateMusicTrack);
router.get("/music-library/files/:fileName", controller.streamMusicFile);

module.exports = router;
