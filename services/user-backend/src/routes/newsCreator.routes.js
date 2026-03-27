const express = require("express");
const router = express.Router();
const controller = require("../controllers/newsCreator.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const {
  generateScriptValidator,
  splitScriptValidator,
  generateSceneAudiosValidator,
  uploadSceneImageValidator,
  createVideoRenderValidator,
} = require("../validator/newsCreator.validator");

router.post(
  "/generate-script",
  authMiddleware,
  generateScriptValidator,
  validate,
  controller.generateScript,
);

router.post(
  "/split-script",
  authMiddleware,
  splitScriptValidator,
  validate,
  controller.splitScript,
);

router.post(
  "/generate-scene-audios",
  authMiddleware,
  generateSceneAudiosValidator,
  validate,
  controller.generateSceneAudios,
);

router.post(
  "/upload-image",
  authMiddleware,
  uploadSceneImageValidator,
  validate,
  controller.uploadSceneImage,
);

router.post(
  "/render-video",
  authMiddleware,
  createVideoRenderValidator,
  validate,
  controller.createVideoRenderJob,
);

router.get(
  "/render-status/:jobId",
  authMiddleware,
  controller.getVideoRenderJobStatus,
);

router.get("/video/:fileName", controller.streamGeneratedVideo);
router.get("/assets/:fileName", controller.streamSceneAsset);

module.exports = router;
