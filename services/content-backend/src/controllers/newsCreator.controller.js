const { response } = require("../utils/response.utils");
const contentService = require("../services/newsCreator.service");
const videoRenderService = require("../services/videoRender.service");

function trimTrailingSlash(value) {
  return String(value || "").replace(/\/+$/, "");
}

function getPublicUserBase(req) {
  const envBase = trimTrailingSlash(process.env.USER_PUBLIC_BASE_URL);
  if (envBase) return envBase;

  const baseUrl = trimTrailingSlash(req.baseUrl || "");
  const normalizedBase =
    baseUrl.toLowerCase().endsWith("/news-content")
      ? baseUrl.slice(0, -"/news-content".length)
      : baseUrl;

  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "http")
    .split(",")[0]
    .trim();
  const host = String(req.headers["x-forwarded-host"] || req.get("host") || "localhost:8000")
    .split(",")[0]
    .trim();

  return `${proto}://${host}${normalizedBase}`;
}

function stripAnsi(text) {
  return String(text || "").replace(
    // eslint-disable-next-line no-control-regex
    /\u001b\[[0-9;]*m/g,
    "",
  );
}

async function generateScript(req, res) {
  try {
    const data = await contentService.generateScript(req.body || {});
    return response(res, 200, "Script generated successfully", data);
  } catch (err) {
    return response(res, 400, err.message || "Unable to generate script");
  }
}

async function convertScriptToHindi(req, res) {
  try {
    const data = await contentService.convertScriptToHindi(req.body || {});
    return response(res, 200, "Hindi script generated successfully", data);
  } catch (err) {
    return response(res, 400, err.message || "Unable to generate Hindi script");
  }
}

async function convertScriptToHindiGemini(req, res) {
  try {
    const data = await contentService.convertScriptToHindiGemini(req.body || {});
    return response(res, 200, "Hindi script generated with Gemini successfully", data);
  } catch (err) {
    return response(
      res,
      400,
      err.message || "Unable to generate Hindi script with Gemini",
      {
        hindiScript: String(err?.partialScript || "").trim(),
      },
    );
  }
}

async function splitScript(req, res) {
  try {
    const data = await contentService.splitScript(req.body || {});
    return response(res, 200, "Script split successfully", data);
  } catch (err) {
    return response(res, 400, err.message || "Unable to split script");
  }
}

async function shortenScript(req, res) {
  try {
    const data = await contentService.shortenScript(req.body || {});
    return response(res, 200, "Script shortened successfully", data);
  } catch (err) {
    return response(res, 400, err.message || "Unable to shorten script");
  }
}

async function generateSceneAudios(req, res) {
  try {
    const data = await contentService.generateSceneAudios(req.body || {});
    const publicBase = getPublicUserBase(req);
    const scenes = Array.isArray(data?.scenes)
      ? data.scenes.map((scene) => ({
        ...scene,
        audioUrl: `${publicBase}/tts/audio/${encodeURIComponent(scene.fileName)}`,
      }))
      : [];
    return response(res, 200, "Scene audio generated successfully", {
      ...data,
      scenes,
    });
  } catch (err) {
    return response(res, 400, err.message || "Unable to generate scene audios");
  }
}

module.exports = {
  generateScript,
  convertScriptToHindi,
  convertScriptToHindiGemini,
  shortenScript,
  splitScript,
  generateSceneAudios,
  uploadSceneImage,
  createVideoRenderJob,
  getVideoRenderJobStatus,
  streamGeneratedVideo,
  streamSceneAsset,
};

async function uploadSceneImage(req, res) {
  try {
    const saved = videoRenderService.saveSceneImage({
      fileName: req.body?.fileName,
      dataUrl: req.body?.dataUrl,
    });
    const publicBase = getPublicUserBase(req);
    return response(res, 201, "Scene image uploaded", {
      fileName: saved.fileName,
      imageUrl: `${publicBase}/news-content/assets/${saved.fileName}`,
    });
  } catch (err) {
    return response(res, 400, err.message || "Unable to upload scene image");
  }
}

async function createVideoRenderJob(req, res) {
  try {
    const job = await videoRenderService.createRenderJob({
      scenes: req.body?.scenes,
      format: req.body?.format,
      title: req.body?.title,
      qualityMode: req.body?.qualityMode,
      renderMode: req.body?.renderMode,
      stylePreset: req.body?.stylePreset,
    });
    return response(res, 202, "Video render job created", job);
  } catch (err) {
    return response(res, 400, err.message || "Unable to create render job");
  }
}

async function getVideoRenderJobStatus(req, res) {
  try {
    const job = videoRenderService.getRenderJob(req.params.jobId);
    const publicBase = getPublicUserBase(req);
    return response(res, 200, "Render job status", {
      id: job.id,
      status: job.status,
      progress: Number(job.progress || 0),
      error: job.error ? stripAnsi(job.error) : null,
      qualityMode: job.qualityMode || "standard",
      estimatedRenderSeconds: Number(job.estimatedRenderSeconds || 0),
      createdAt: job.createdAt,
      finishedAt: job.finishedAt,
      fileName: job.fileName,
      lastStdout: job.lastStdout || "",
      lastStderr: job.lastStderr || "",
      videoUrl: job.status === "completed" ? `${publicBase}/news-content/video/${job.fileName}` : null,
    });
  } catch (err) {
    return response(res, 404, err.message || "Render job not found");
  }
}

async function streamGeneratedVideo(req, res) {
  try {
    const filePath = videoRenderService.getVideoPath(req.params.fileName);
    if (!require("fs").existsSync(filePath)) {
      return response(res, 404, "Video file not found");
    }
    res.setHeader("Content-Type", "video/mp4");
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err.message || "Unable to read video file");
  }
}

async function streamSceneAsset(req, res) {
  try {
    const filePath = videoRenderService.getAssetPath(req.params.fileName);
    if (!require("fs").existsSync(filePath)) {
      return response(res, 404, "Asset file not found");
    }

    const ext = String(req.params.fileName || "").toLowerCase();
    if (ext.endsWith(".jpg") || ext.endsWith(".jpeg")) {
      res.setHeader("Content-Type", "image/jpeg");
    } else if (ext.endsWith(".webp")) {
      res.setHeader("Content-Type", "image/webp");
    } else {
      res.setHeader("Content-Type", "image/png");
    }
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err.message || "Unable to read asset file");
  }
}
