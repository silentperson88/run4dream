const express = require("express");
const router = express.Router();
const controller = require("../controllers/audioTools.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { authMiddleware } = require("../middlewares/auth.middleware");
const {
  processAudioValidator,
  processedAudioFileNameParam,
  savePresetValidator,
  presetIdParam,
} = require("../validator/audioTools.validator");

router.post(
  "/audio-tools/process",
  authMiddleware,
  processAudioValidator,
  validate,
  controller.processAudio,
);

router.post(
  "/audio-tools/presets",
  authMiddleware,
  savePresetValidator,
  validate,
  controller.savePreset,
);

router.get(
  "/audio-tools/presets",
  authMiddleware,
  controller.listPresets,
);

router.get(
  "/audio-tools/presets/:id",
  authMiddleware,
  presetIdParam,
  validate,
  controller.getPreset,
);

router.get(
  "/audio-tools/generated/:fileName",
  processedAudioFileNameParam,
  validate,
  controller.streamProcessedAudio,
);

module.exports = router;
