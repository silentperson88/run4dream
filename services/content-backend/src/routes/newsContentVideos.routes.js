const express = require("express");
const router = express.Router();
const controller = require("../controllers/newsContentVideos.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const {
  listVideosValidator,
  createVideoValidator,
  updateVideoValidator,
  uploadAssetValidator,
  videoIdParam,
  streamAssetValidator,
  renderFfmpegGenerateValidator,
  renderRemotionPreviewGenerateValidator,
  fastGpuRenderCreateValidator,
  fastGpuRenderJobIdParam,
  keywordValidator,
} = require("../validator/newsContentVideos.validator");

router.get(
  "/videos",
  authMiddleware,
  listVideosValidator,
  validate,
  controller.listVideos,
);

router.post(
  "/videos",
  authMiddleware,
  createVideoValidator,
  validate,
  controller.createVideo,
);

router.post(
  "/videos/keyword",
  authMiddleware,
  keywordValidator,
  validate,
  controller.generateImageKeyword,
);

router.get(
  "/videos/:id",
  authMiddleware,
  videoIdParam,
  validate,
  controller.getVideo,
);

router.put(
  "/videos/:id",
  authMiddleware,
  updateVideoValidator,
  validate,
  controller.updateVideo,
);

router.post(
  "/videos/:id/assets/image",
  authMiddleware,
  uploadAssetValidator,
  validate,
  controller.uploadImage,
);

router.post(
  "/videos/:id/assets/audio",
  authMiddleware,
  uploadAssetValidator,
  validate,
  controller.uploadAudio,
);

router.post(
  "/videos/:id/render",
  authMiddleware,
  videoIdParam,
  validate,
  controller.renderVideo,
);

router.post(
  "/ffmpeg/generate-video",
  authMiddleware,
  renderFfmpegGenerateValidator,
  validate,
  controller.generateFfmpegVideo,
);

router.post(
  "/remotion-preview/generate-video",
  authMiddleware,
  renderRemotionPreviewGenerateValidator,
  validate,
  controller.generateRemotionPreviewVideo,
);

router.post(
  "/v2/gpu-render/jobs",
  authMiddleware,
  fastGpuRenderCreateValidator,
  validate,
  controller.createFastGpuRenderJob,
);

router.get(
  "/v2/gpu-render/jobs",
  authMiddleware,
  controller.listFastGpuRenderJobs,
);

router.get(
  "/v2/gpu-render/jobs/:jobId",
  authMiddleware,
  fastGpuRenderJobIdParam,
  validate,
  controller.getFastGpuRenderJob,
);


router.get(
  "/videos/:id/render-status/:jobId",
  authMiddleware,
  videoIdParam,
  validate,
  controller.renderStatus,
);

router.get(
  "/videos/:id/render-status-ffmpeg/:jobId",
  authMiddleware,
  videoIdParam,
  validate,
  controller.renderStatus,
);

router.get(
  "/videos/:id/render-status-remotion-preview/:jobId",
  authMiddleware,
  videoIdParam,
  validate,
  controller.renderStatusRemotionPreview,
);


router.get(
  "/videos/rendered/:fileName",
  controller.streamRenderedVideo,
);

router.get(
  "/videos/assets/images/:fileName",
  controller.streamImage,
);

router.get(
  "/videos/assets/audio/:fileName",
  controller.streamAudio,
);

module.exports = router;
