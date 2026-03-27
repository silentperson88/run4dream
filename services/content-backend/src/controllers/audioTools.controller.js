const fs = require("fs");
const { response } = require("../utils/response.utils");
const service = require("../services/audioTools.service");

async function processAudio(req, res) {
  try {
    const data = await service.processAudioModification({
      audioDataUrl: req.body?.audioDataUrl,
      sourceUrl: req.body?.sourceUrl,
      fileName: req.body?.fileName,
      preset: req.body?.preset,
      outputFormat: req.body?.outputFormat,
      options: req.body?.options || {},
    });
    return response(res, 200, "Audio processed", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to process audio");
  }
}

async function streamProcessedAudio(req, res) {
  try {
    const filePath = service.getProcessedAudioPath(req.params?.fileName);
    if (!fs.existsSync(filePath)) return response(res, 404, "Audio not found");
    res.setHeader("Content-Type", service.getProcessedAudioContentType(req.params?.fileName));
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to read audio");
  }
}

async function savePreset(req, res) {
  try {
    const data = await service.savePreset({
      userId: Number(req.user?.id || 0),
      presetName: req.body?.presetName,
      presetConfig: req.body?.presetConfig || {},
    });
    return response(res, 200, "Preset saved", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to save preset");
  }
}

async function listPresets(req, res) {
  try {
    const data = await service.listPresets({
      userId: Number(req.user?.id || 0),
    });
    return response(res, 200, "Presets list", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to list presets");
  }
}

async function getPreset(req, res) {
  try {
    const data = await service.getPreset({
      userId: Number(req.user?.id || 0),
      id: req.params?.id,
    });
    if (!data) return response(res, 404, "Preset not found");
    return response(res, 200, "Preset", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch preset");
  }
}

module.exports = {
  processAudio,
  streamProcessedAudio,
  savePreset,
  listPresets,
  getPreset,
};
