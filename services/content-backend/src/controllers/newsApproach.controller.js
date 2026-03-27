const { response } = require("../utils/response.utils");
const service = require("../services/newsApproach.service");
const fs = require("fs");

async function summarizeByNewsId(req, res) {
  try {
    const data = await service.summarizeNewsPdf({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      model: req.body?.model,
    });
    return response(res, 200, "Summary generated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate summary");
  }
}

async function importantPoints(req, res) {
  try {
    const data = await service.extractImportantPoints({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      summary: req.body?.summary,
      model: req.body?.model,
    });
    return response(res, 200, "Important points generated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate important points");
  }
}

async function generateScript(req, res) {
  try {
    const data = await service.generateScriptFromPoints({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      points: req.body?.points,
      model: req.body?.model,
      language: req.body?.language,
    });
    return response(res, 200, "Script generated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate script");
  }
}

async function generateStructuredScript(req, res) {
  try {
    const data = await service.generateStructuredScriptFromPoints({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      points: req.body?.points,
      model: req.body?.model,
      language: req.body?.language,
      targetDurationSec: req.body?.targetDurationSec,
      wordCountMin: req.body?.wordCountMin,
      wordCountMax: req.body?.wordCountMax,
    });
    return response(res, 200, "Structured script generated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate structured script");
  }
}

async function generateScriptAudio(req, res) {
  try {
    const data = await service.generateScriptAudio({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      script: req.body?.script,
      language: req.body?.language,
      model: req.body?.model,
      tuning: req.body?.tuning,
      options: req.body?.options,
    });
    return response(res, 200, "Script audio generated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate script audio");
  }
}

async function extractHighlightTerms(req, res) {
  try {
    const data = await service.extractHighlightTerms({
      userId: Number(req.user?.id || 0),
      newsId: req.body?.newsId,
      script: req.body?.script,
      language: req.body?.language,
      model: req.body?.model,
    });
    return response(res, 200, "Highlight terms extracted", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to extract highlight terms");
  }
}

async function startBatch(req, res) {
  try {
    const data = await service.startBatchGeneration({
      userId: Number(req.user?.id || 0),
      date: req.body?.date,
      category: req.body?.category,
      model: req.body?.model,
      gapMs: req.body?.gapMs,
    });
    return response(res, 200, "Batch started", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to start batch");
  }
}

async function getBatchStatus(req, res) {
  try {
    const data = await service.getBatchStatus({
      userId: Number(req.user?.id || 0),
      jobId: req.params?.jobId,
    });
    return response(res, 200, "Batch status", data);
  } catch (err) {
    return response(res, 404, err?.message || "Batch job not found");
  }
}

async function stopBatch(req, res) {
  try {
    const data = await service.stopBatchGeneration({
      userId: Number(req.user?.id || 0),
      jobId: req.params?.jobId,
    });
    return response(res, 200, "Batch stop requested", data);
  } catch (err) {
    return response(res, 404, err?.message || "Batch job not found");
  }
}

async function streamNewsApproachAudio(req, res) {
  try {
    const filePath = service.getNewsApproachAudioPath(req.params.fileName);
    if (!fs.existsSync(filePath)) {
      return response(res, 404, "Audio file not found");
    }
    res.setHeader("Content-Type", "audio/wav");
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to read audio file");
  }
}

module.exports = {
  summarizeByNewsId,
  importantPoints,
  generateScript,
  generateStructuredScript,
  generateScriptAudio,
  extractHighlightTerms,
  startBatch,
  getBatchStatus,
  stopBatch,
  streamNewsApproachAudio,
};
