const { body } = require("express-validator");

const generateScriptValidator = [
  body("extraInfoType")
    .optional()
    .isIn(["GENERAL_CONTEXT", "SUBTOPIC_LIST", "CONTEXT_PLUS_SUBTOPICS"])
    .withMessage("extraInfoType must be GENERAL_CONTEXT, SUBTOPIC_LIST, or CONTEXT_PLUS_SUBTOPICS"),
  body("topic")
    .trim()
    .notEmpty()
    .withMessage("topic is required")
    .isLength({ min: 3, max: 300 })
    .withMessage("topic must be between 3 and 300 characters"),
  body("extraInfo")
    .optional()
    .isString()
    .withMessage("extraInfo must be a string")
    .isLength({ max: 200000 })
    .withMessage("extraInfo must be up to 200000 characters"),
  body("context")
    .optional()
    .isString()
    .withMessage("context must be a string")
    .isLength({ max: 200000 })
    .withMessage("context must be up to 200000 characters"),
  body("platform")
    .optional()
    .isString()
    .withMessage("platform must be a string")
    .isLength({ max: 50 })
    .withMessage("platform must be up to 50 characters"),
  body("tone")
    .optional()
    .isString()
    .withMessage("tone must be a string")
    .isLength({ max: 120 })
    .withMessage("tone must be up to 120 characters"),
  body("language")
    .optional()
    .isString()
    .withMessage("language must be a string")
    .isLength({ max: 20 })
    .withMessage("language must be up to 20 characters"),
  body("targetDurationSec")
    .optional()
    .isInt({ min: 20, max: 900 })
    .withMessage("targetDurationSec must be between 20 and 900"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
  body("scriptLength")
    .optional()
    .isIn(["short", "long"])
    .withMessage("scriptLength must be short or long"),
  body("scriptWordsMin")
    .optional()
    .isInt({ min: 30, max: 8000 })
    .withMessage("scriptWordsMin must be between 30 and 8000"),
  body("scriptWordsMax")
    .optional()
    .isInt({ min: 30, max: 8000 })
    .withMessage("scriptWordsMax must be between 30 and 8000"),
  body("subtopics")
    .optional()
    .isArray({ min: 1, max: 30 })
    .withMessage("subtopics must be an array with 1 to 30 items"),
  body("subtopics.*")
    .optional()
    .isString()
    .withMessage("each subtopic must be a string")
    .isLength({ min: 2, max: 160 })
    .withMessage("each subtopic must be between 2 and 160 characters"),
];

const convertScriptToHindiValidator = [
  body("script")
    .trim()
    .notEmpty()
    .withMessage("script is required")
    .isLength({ min: 30, max: 25000 })
    .withMessage("script must be between 30 and 25000 characters"),
  body("topic")
    .optional()
    .isString()
    .withMessage("topic must be a string")
    .isLength({ max: 300 })
    .withMessage("topic must be up to 300 characters"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const convertScriptToHindiGeminiValidator = [
  body("script")
    .trim()
    .notEmpty()
    .withMessage("script is required")
    .isLength({ min: 30, max: 25000 })
    .withMessage("script must be between 30 and 25000 characters"),
  body("topic")
    .optional()
    .isString()
    .withMessage("topic must be a string")
    .isLength({ max: 300 })
    .withMessage("topic must be up to 300 characters"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const splitScriptValidator = [
  body("extraInfoType")
    .optional()
    .isIn(["GENERAL_CONTEXT", "SUBTOPIC_LIST", "CONTEXT_PLUS_SUBTOPICS"])
    .withMessage("extraInfoType must be GENERAL_CONTEXT, SUBTOPIC_LIST, or CONTEXT_PLUS_SUBTOPICS"),
  body("script")
    .trim()
    .notEmpty()
    .withMessage("script is required")
    .isLength({ min: 30, max: 25000 })
    .withMessage("script must be between 30 and 25000 characters"),
  body("sceneCount")
    .optional()
    .isInt({ min: 2, max: 20 })
    .withMessage("sceneCount must be between 2 and 20"),
  body("platform")
    .optional()
    .isString()
    .withMessage("platform must be a string")
    .isLength({ max: 50 })
    .withMessage("platform must be up to 50 characters"),
  body("tone")
    .optional()
    .isString()
    .withMessage("tone must be a string")
    .isLength({ max: 120 })
    .withMessage("tone must be up to 120 characters"),
  body("language")
    .optional()
    .isString()
    .withMessage("language must be a string")
    .isLength({ max: 20 })
    .withMessage("language must be up to 20 characters"),
  body("targetDurationSec")
    .optional()
    .isInt({ min: 20, max: 900 })
    .withMessage("targetDurationSec must be between 20 and 900"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
  body("subtopics")
    .optional()
    .isArray({ min: 1, max: 30 })
    .withMessage("subtopics must be an array with 1 to 30 items"),
  body("subtopics.*")
    .optional()
    .isString()
    .withMessage("each subtopic must be a string")
    .isLength({ min: 2, max: 160 })
    .withMessage("each subtopic must be between 2 and 160 characters"),
  body("extraInfo")
    .optional()
    .isString()
    .withMessage("extraInfo must be a string")
    .isLength({ max: 200000 })
    .withMessage("extraInfo must be up to 200000 characters"),
];

const shortenScriptValidator = [
  body("script")
    .trim()
    .notEmpty()
    .withMessage("script is required")
    .isLength({ min: 30, max: 25000 })
    .withMessage("script must be between 30 and 25000 characters"),
  body("language")
    .optional()
    .isString()
    .withMessage("language must be a string")
    .isLength({ max: 20 })
    .withMessage("language must be up to 20 characters"),
  body("targetDurationSec")
    .optional()
    .isInt({ min: 15, max: 300 })
    .withMessage("targetDurationSec must be between 15 and 300"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 100 })
    .withMessage("model must be up to 100 characters"),
];

const generateSceneAudiosValidator = [
  body("scenes")
    .isArray({ min: 1, max: 30 })
    .withMessage("scenes must be an array with 1 to 30 items"),
  body("scenes.*.narration")
    .optional()
    .isString()
    .withMessage("scene narration must be a string")
    .isLength({ min: 1, max: 200000 })
    .withMessage("scene narration must be between 1 and 200000 characters"),
  body("scenes.*.text")
    .optional()
    .isString()
    .withMessage("scene text must be a string")
    .isLength({ min: 1, max: 200000 })
    .withMessage("scene text must be between 1 and 200000 characters"),
  body("language")
    .optional()
    .isIn(["en", "hi"])
    .withMessage("language must be en or hi"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ max: 40 })
    .withMessage("model must be up to 40 characters"),
  body("tuning").optional().isObject().withMessage("tuning must be an object"),
  body("options").optional().isObject().withMessage("options must be an object"),
];

const uploadSceneImageValidator = [
  body("fileName")
    .optional()
    .isString()
    .withMessage("fileName must be a string")
    .isLength({ max: 120 })
    .withMessage("fileName must be up to 120 characters"),
  body("dataUrl")
    .isString()
    .withMessage("dataUrl is required")
    .matches(/^data:image\/[a-zA-Z0-9.+-]+;base64,/)
    .withMessage("dataUrl must be a valid base64 image data URL"),
];

const createVideoRenderValidator = [
  body("title")
    .optional()
    .isString()
    .withMessage("title must be a string")
    .isLength({ max: 200 })
    .withMessage("title must be up to 200 characters"),
  body("format")
    .optional()
    .isIn(["vertical", "landscape", "square"])
    .withMessage("format must be vertical, landscape, or square"),
  body("qualityMode")
    .optional()
    .isIn(["draft", "standard", "high"])
    .withMessage("qualityMode must be draft, standard, or high"),
  body("renderMode")
    .optional()
    .isIn(["scene", "text_news", "news_sequence", "news_approach_1"])
    .withMessage("renderMode must be scene, text_news, news_sequence, or news_approach_1"),
  body("stylePreset")
    .optional()
    .isIn(["flash", "data", "story"])
    .withMessage("stylePreset must be flash, data, or story"),
  body("scenes")
    .isArray({ min: 1, max: 60 })
    .withMessage("scenes must be an array with 1 to 60 items"),
  body("scenes.*.durationSec")
    .optional()
    .isFloat({ min: 1, max: 1200 })
    .withMessage("scene durationSec must be between 1 and 1200"),
  body("scenes.*.imageUrl")
    .optional()
    .isString()
    .withMessage("scene imageUrl must be a string")
    .isLength({ max: 2000 })
    .withMessage("scene imageUrl must be up to 2000 chars"),
  body("scenes.*.audioUrl")
    .optional()
    .isString()
    .withMessage("scene audioUrl must be a string")
    .isLength({ max: 2000 })
    .withMessage("scene audioUrl must be up to 2000 chars"),
];

module.exports = {
  generateScriptValidator,
  convertScriptToHindiValidator,
  convertScriptToHindiGeminiValidator,
  shortenScriptValidator,
  splitScriptValidator,
  generateSceneAudiosValidator,
  uploadSceneImageValidator,
  createVideoRenderValidator,
};
