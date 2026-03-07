const fs = require("fs");
const ttsService = require("../services/tts.service");
const { response } = require("../utils/response.utils");

async function generateTtsAudio(req, res) {
  try {
    const text = String(req.body?.text || "").trim();
    const language = String(req.body?.language || "").trim().toLowerCase();
    const model = String(req.body?.model || "").trim().toLowerCase();
    const tuning = {
      speed: req.body?.speed,
      noiseScale: req.body?.noiseScale,
      noiseW: req.body?.noiseW,
      sentencePause: req.body?.sentencePause,
    };
    const options = {
      normalizeText: req.body?.normalizeText,
      splitSentences: req.body?.splitSentences,
    };

    if (!text) {
      return response(res, 400, "Text is required");
    }

    if (text.length < 10) {
      return response(res, 400, "Please provide at least 10 characters");
    }

    const data = await ttsService.generateAudio({ text, language, model, tuning, options });

    return response(res, 201, "Audio generated successfully", {
      fileName: data.fileName,
      language,
      model,
      audioUrl: `user/tts/audio/${data.fileName}`,
      absoluteFilePath: data.filePath,
      appliedTuning: data.tuning,
      appliedOptions: data.options,
      defaultTuning: ttsService.DEFAULT_TUNING,
      defaultOptions: ttsService.DEFAULT_OPTIONS,
      availableModels: Object.keys(ttsService.MODEL_CATALOG?.[language] || {}),
    });
  } catch (err) {
    return response(res, 400, err.message || "Unable to generate audio");
  }
}

async function streamGeneratedAudio(req, res) {
  try {
    const filePath = ttsService.getGeneratedAudioPath(req.params.fileName);
    if (!fs.existsSync(filePath)) {
      return response(res, 404, "Audio file not found");
    }

    res.setHeader("Content-Type", "audio/wav");
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err.message || "Unable to read audio file");
  }
}

module.exports = {
  generateTtsAudio,
  streamGeneratedAudio,
};
