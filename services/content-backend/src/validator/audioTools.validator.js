const { body, param } = require("express-validator");

const processAudioValidator = [
  body("audioDataUrl").optional().isString().withMessage("audioDataUrl must be a string"),
  body("sourceUrl").optional().isString().withMessage("sourceUrl must be a string"),
  body("fileName").optional().isString().withMessage("fileName must be a string"),
  body("preset")
    .optional()
    .isIn(["original", "broadcast", "warm_room", "radio", "cinematic", "lofi", "noisy_tv", "clean"])
    .withMessage("preset invalid"),
  body("outputFormat").optional().isIn(["mp3", "wav"]).withMessage("outputFormat must be mp3 or wav"),
  body("options").optional().isObject().withMessage("options must be an object"),
  body().custom((_, { req }) => {
    if (!req.body?.audioDataUrl && !req.body?.sourceUrl) {
      throw new Error("audioDataUrl or sourceUrl is required");
    }
    return true;
  }),
];

const processedAudioFileNameParam = [
  param("fileName").isString().withMessage("fileName is required"),
];

const savePresetValidator = [
  body("presetName").isString().trim().notEmpty().withMessage("presetName is required"),
  body("presetConfig").isObject().withMessage("presetConfig must be an object"),
];

const presetIdParam = [
  param("id").isInt({ min: 1 }).withMessage("id must be a positive integer"),
];

module.exports = {
  processAudioValidator,
  processedAudioFileNameParam,
  savePresetValidator,
  presetIdParam,
};
