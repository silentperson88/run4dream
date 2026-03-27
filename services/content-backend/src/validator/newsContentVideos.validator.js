const { body, param, query } = require("express-validator");

const listVideosValidator = [
  query("limit").optional().isInt({ min: 1, max: 200 }).withMessage("limit must be 1-200"),
  query("offset").optional().isInt({ min: 0, max: 100000 }).withMessage("offset must be 0-100000"),
];

const createVideoValidator = [
  body("language").optional().isIn(["english", "hindi"]).withMessage("language must be english or hindi"),
];

const videoIdParam = [
  param("id").isInt({ min: 1 }).withMessage("id must be a positive integer"),
];

const updateVideoValidator = [
  ...videoIdParam,
  body("language").optional().isIn(["english", "hindi"]).withMessage("language must be english or hindi"),
  body("script").optional().isString().withMessage("script must be a string"),
  body("clipApproach").optional().isIn(["multi_sentence", "single_sentence"]).withMessage("clipApproach invalid"),
  body("sentenceKeywords").optional().isObject().withMessage("sentenceKeywords must be an object"),
  body("audioUrl").optional().isString().withMessage("audioUrl must be a string"),
  body("clips").optional().isArray().withMessage("clips must be an array"),
  body("status").optional().isIn(["draft", "finished", "published", "ready_for_download", "rendering"]).withMessage("status invalid"),
];

const renderFfmpegGenerateValidator = [
  body("videoId").isInt({ min: 1 }).withMessage("videoId must be a positive integer"),
  body("format").optional().isIn(["landscape", "short"]).withMessage("format must be landscape or short"),
  body("resolution").optional().isIn(["720p", "1080p"]).withMessage("resolution must be 720p or 1080p"),
  body("fps").optional().isInt({ min: 12, max: 60 }).withMessage("fps must be 12-60"),
  body("perSentenceSec").optional().isFloat({ min: 0.2, max: 60 }).withMessage("perSentenceSec must be 0.2-60"),
  body("qualityMode").optional().isIn(["draft", "standard", "high"]).withMessage("qualityMode invalid"),
  body("timeline").optional().isObject().withMessage("timeline must be an object"),
  body("timeline.segments").optional().isArray().withMessage("timeline.segments must be an array"),
  body("timeline.overlays").optional().isArray().withMessage("timeline.overlays must be an array"),
];

const renderRemotionPreviewGenerateValidator = [
  body("videoId").isInt({ min: 1 }).withMessage("videoId must be a positive integer"),
  body("format").optional().isIn(["landscape", "short"]).withMessage("format must be landscape or short"),
  body("resolution").optional().isIn(["720p", "1080p"]).withMessage("resolution must be 720p or 1080p"),
  body("durationInFrames").isInt({ min: 1 }).withMessage("durationInFrames must be positive"),
  body("renderFrameEnd").optional().isInt({ min: 0 }).withMessage("renderFrameEnd must be >= 0"),
  body("script")
    .optional()
    .isString()
    .withMessage("script must be a string"),
  body("audioUrl")
    .optional()
    .isString()
    .withMessage("audioUrl must be a string"),
  body("previewProps")
    .optional()
    .isObject()
    .withMessage("previewProps must be an object"),
  body().custom((_, { req }) => {
    const script = String(req.body?.script || req.body?.previewProps?.script || "").trim();
    const audioUrl = String(req.body?.audioUrl || req.body?.previewProps?.audioUrl || "").trim();
    if (!script) throw new Error("script is required");
    if (!audioUrl) throw new Error("audioUrl is required");
    return true;
  }),
  body("sentences").optional().isArray().withMessage("sentences must be an array"),
  body("sentenceFrames").optional().isArray().withMessage("sentenceFrames must be an array"),
  body("clips").optional().isArray().withMessage("clips must be an array"),
  body("qualityMode").optional().isIn(["draft", "standard", "high", "gpu"]).withMessage("qualityMode invalid"),
];

const fastGpuRenderCreateValidator = [
  body("videoId").isInt({ min: 1 }).withMessage("videoId must be a positive integer"),
  body("format").optional().isIn(["landscape", "short"]).withMessage("format must be landscape or short"),
  body("resolution").optional().isIn(["720p", "1080p"]).withMessage("resolution must be 720p or 1080p"),
  body("durationInFrames").isInt({ min: 1 }).withMessage("durationInFrames must be positive"),
  body("renderFrameEnd").optional().isInt({ min: 0 }).withMessage("renderFrameEnd must be >= 0"),
  body("qualityMode").optional().isIn(["draft", "standard", "high", "gpu"]).withMessage("qualityMode invalid"),
  body("previewProps").optional().isObject().withMessage("previewProps must be an object"),
  body().custom((_, { req }) => {
    const script = String(req.body?.script || req.body?.previewProps?.script || "").trim();
    const audioUrl = String(req.body?.audioUrl || req.body?.previewProps?.audioUrl || "").trim();
    if (!script) throw new Error("script is required");
    if (!audioUrl) throw new Error("audioUrl is required");
    return true;
  }),
];

const fastGpuRenderJobIdParam = [
  param("jobId").isUUID().withMessage("jobId must be a valid uuid"),
];

const keywordValidator = [
  body("sentence").isString().withMessage("sentence is required"),
];

const uploadAssetValidator = [
  ...videoIdParam,
  body("dataUrl").optional().isString().withMessage("dataUrl must be a string"),
  body("sourceUrl").optional().isString().withMessage("sourceUrl must be a string"),
  body("fileName").optional().isString().withMessage("fileName must be a string"),
  body().custom((_, { req }) => {
    if (!req.body?.dataUrl && !req.body?.sourceUrl) {
      throw new Error("dataUrl or sourceUrl is required");
    }
    return true;
  }),
];

const streamAssetValidator = [
  param("fileName").isString().withMessage("fileName is required"),
];

module.exports = {
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
};
