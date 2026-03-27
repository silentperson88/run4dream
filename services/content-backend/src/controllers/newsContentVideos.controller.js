const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { pathToFileURL } = require("url");

function audioFileToDataUrl(filePath) {
  try {
    const ext = String(filePath || "").toLowerCase();
    const mime = ext.endsWith(".mp3") ? "audio/mpeg" : "audio/wav";
    const bin = fs.readFileSync(filePath);
    return `data:${mime};base64,${bin.toString("base64")}`;
  } catch (_) {
    return "";
  }
}

function imageFileToDataUrl(filePath) {
  try {
    const ext = String(filePath || "").toLowerCase();
    const mime =
      ext.endsWith(".jpg") || ext.endsWith(".jpeg")
        ? "image/jpeg"
        : ext.endsWith(".webp")
          ? "image/webp"
          : "image/png";
    const bin = fs.readFileSync(filePath);
    return `data:${mime};base64,${bin.toString("base64")}`;
  } catch (_) {
    return "";
  }
}
const { response } = require("../utils/response.utils");
const service = require("../services/newsContentVideos.service");
const videoRenderService = require("../services/videoRender.service");
const remotionPreviewRenderService = require("../services/remotionPreviewRender.service");
const gpuFastRenderV2Service = require("../services/gpuFastRenderV2.service");
const { chatWithOllama } = require("../services/ollama.service");

function splitSentences(text) {
  return String(text || "")
    .split(/[.!?\u0964|]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function buildSentenceFrames(sentences, totalFrames) {
  const safeSentences = Array.isArray(sentences) ? sentences : [];
  const lineCount = Math.max(1, safeSentences.length || 1);
  const safeTotalFrames = Math.max(1, Math.round(Number(totalFrames || 1)));
  const wordCounts = safeSentences.map((s) => countWords(s));
  const totalWords = wordCounts.reduce((sum, count) => sum + count, 0);
  if (!totalWords) {
    const fallback = Math.max(6, Math.floor(safeTotalFrames / lineCount));
    return new Array(lineCount).fill(fallback);
  }
  const baseFrames = wordCounts.map((count) =>
    Math.max(1, Math.round((count / totalWords) * safeTotalFrames)),
  );
  const sumFrames = baseFrames.reduce((sum, count) => sum + count, 0);
  const diff = safeTotalFrames - sumFrames;
  if (diff === 0) return baseFrames;
  const next = baseFrames.slice();
  const step = diff > 0 ? 1 : -1;
  for (let i = 0; i < Math.abs(diff); i += 1) {
    const idx = i % next.length;
    next[idx] = Math.max(1, next[idx] + step);
  }
  return next;
}

function buildSentenceBoundaries(frames) {
  const boundaries = [];
  let acc = 0;
  (frames || []).forEach((count) => {
    acc += Number(count || 0);
    boundaries.push(acc);
  });
  return boundaries;
}

function buildSeedValue(seedKey) {
  const text = String(seedKey || "");
  let seed = 0;
  for (let i = 0; i < text.length; i += 1) {
    seed = (seed * 31 + text.charCodeAt(i)) % 100000;
  }
  return seed;
}

function escapeTextForFfmpeg(text) {
  return String(text || "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'")
    .replace(/,/g, "\\,")
    .replace(/\n/g, " ");
}

function countWords(text) {
  return String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function normalizeSentenceDurations(rawDurations, totalDurationSec, minSec) {
  const safeMin = Math.max(0, Number(minSec || 0));
  const base = rawDurations.map((val) => Math.max(0, Number(val || 0)));
  if (!base.length) return [];
  let adjusted = base.map((val) => Math.max(safeMin, val));
  const sum = adjusted.reduce((acc, val) => acc + val, 0) || 1;
  if (!Number.isFinite(totalDurationSec) || totalDurationSec <= 0)
    return adjusted;

  const diff = sum - totalDurationSec;
  if (Math.abs(diff) < 0.001) return adjusted;

  if (diff > 0) {
    const adjustable = adjusted.map((val) => Math.max(0, val - safeMin));
    const adjustableSum = adjustable.reduce((acc, val) => acc + val, 0);
    if (adjustableSum > 0) {
      adjusted = adjusted.map((val, idx) => {
        if (val <= safeMin) return val;
        const shrink = (adjustable[idx] / adjustableSum) * diff;
        return Math.max(safeMin, val - shrink);
      });
    }
  } else {
    const grow = Math.abs(diff);
    adjusted = adjusted.map((val) => val + (val / sum) * grow);
  }

  return adjusted;
}

async function getAudioDurationSec(audioPath) {
  if (!audioPath || !fs.existsSync(audioPath)) return 0;
  return await new Promise((resolve) => {
    const ffprobe = spawn("ffprobe", [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      audioPath,
    ]);
    let out = "";
    ffprobe.stdout.on("data", (chunk) => {
      out += chunk.toString();
    });
    ffprobe.on("error", () => resolve(0));
    ffprobe.on("close", () => {
      const val = Number(String(out || "").trim());
      resolve(Number.isFinite(val) ? val : 0);
    });
  });
}

function resolveAssetPath(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  const relative =
    raw.startsWith("http://") || raw.startsWith("https://")
      ? raw
      : `/${raw.replace(/^\/+/, "")}`;
  const match = relative.match(
    /\/content\/news-content\/videos\/assets\/images\/([^/?#]+)/i,
  );
  if (match?.[1]) {
    const fileName = decodeURIComponent(match[1]);
    const filePath = service.getImagePath(fileName);
    if (fs.existsSync(filePath)) return filePath;
  }
  return raw;
}

function resolveAudioPath(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  const relative =
    raw.startsWith("http://") || raw.startsWith("https://")
      ? raw
      : `/${raw.replace(/^\/+/, "")}`;
  const match = relative.match(
    /\/content\/news-content\/videos\/assets\/audio\/([^/?#]+)/i,
  );
  if (match?.[1]) {
    const fileName = decodeURIComponent(match[1]);
    const filePath = service.getAudioPath(fileName);
    if (fs.existsSync(filePath)) return filePath;
  }
  return raw;
}

async function listVideos(req, res) {
  try {
    const data = await service.listVideos({
      userId: Number(req.user?.id || 0),
      limit: req.query?.limit,
      offset: req.query?.offset,
    });
    return response(res, 200, "Videos list", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to list videos");
  }
}

async function createVideo(req, res) {
  try {
    const data = await service.createVideo({
      userId: Number(req.user?.id || 0),
      language: req.body?.language,
    });
    return response(res, 201, "Video created", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to create video");
  }
}

async function getVideo(req, res) {
  try {
    const data = await service.getVideo({
      userId: Number(req.user?.id || 0),
      id: req.params?.id,
    });
    if (!data) return response(res, 404, "Video not found");
    return response(res, 200, "Video", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch video");
  }
}

async function updateVideo(req, res) {
  try {
    const data = await service.updateVideo({
      userId: Number(req.user?.id || 0),
      id: req.params?.id,
      language: req.body?.language,
      script: req.body?.script,
      clipApproach: req.body?.clipApproach,
      sentenceKeywords: req.body?.sentenceKeywords,
      audioUrl: req.body?.audioUrl,
      clips: req.body?.clips,
      status: req.body?.status,
    });
    if (!data) return response(res, 404, "Video not found");
    return response(res, 200, "Video updated", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to update video");
  }
}

async function uploadImage(req, res) {
  try {
    const data = await service.saveImageAsset({
      userId: Number(req.user?.id || 0),
      videoId: req.params?.id,
      dataUrl: req.body?.dataUrl,
      sourceUrl: req.body?.sourceUrl,
      fileName: req.body?.fileName,
    });
    return response(res, 200, "Image saved", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to save image");
  }
}

async function uploadAudio(req, res) {
  try {
    const data = await service.saveAudioAsset({
      userId: Number(req.user?.id || 0),
      videoId: req.params?.id,
      dataUrl: req.body?.dataUrl,
      fileName: req.body?.fileName,
    });
    return response(res, 200, "Audio saved", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to save audio");
  }
}

async function renderVideo(req, res) {
  try {
    const id = Number(req.params?.id || 0);
    const video = await service.getVideo({
      userId: Number(req.user?.id || 0),
      id,
    });
    if (!video) return response(res, 404, "Video not found");

    const forwardedProto = req.headers["x-forwarded-proto"];
    const forwardedHost = req.headers["x-forwarded-host"];
    const proto = Array.isArray(forwardedProto)
      ? forwardedProto[0]
      : forwardedProto || req.protocol;
    const host = Array.isArray(forwardedHost)
      ? forwardedHost[0]
      : forwardedHost || req.get("host");
    const requestBase = proto && host ? `${proto}://${host}` : "";
    const mediaBase =
      process.env.RENDER_MEDIA_BASE_URL ||
      requestBase ||
      `http://127.0.0.1:${Number(process.env.PORT || 8001)}`;
    const toAbsolute = (url) => {
      const raw = String(url || "").trim();
      if (!raw) return "";
      const relativePath =
        raw.startsWith("http://") || raw.startsWith("https://")
          ? raw
          : `/${raw.replace(/^\/+/, "")}`;
      const audioMatch = relativePath.match(
        /\/content\/news-content\/videos\/assets\/audio\/([^/?#]+)/i,
      );
      if (audioMatch?.[1]) {
        const fileName = decodeURIComponent(audioMatch[1]);
        const filePath = service.getAudioPath(fileName);
        if (fs.existsSync(filePath)) {
          const dataUrl = audioFileToDataUrl(filePath);
          return dataUrl || pathToFileURL(filePath).href;
        }
      }
      const imageMatch = relativePath.match(
        /\/content\/news-content\/videos\/assets\/images\/([^/?#]+)/i,
      );
      if (imageMatch?.[1]) {
        const fileName = decodeURIComponent(imageMatch[1]);
        const filePath = service.getImagePath(fileName);
        if (fs.existsSync(filePath)) {
          const dataUrl = imageFileToDataUrl(filePath);
          return dataUrl || pathToFileURL(filePath).href;
        }
      }
      if (raw.startsWith("http://") || raw.startsWith("https://")) {
        if (raw.includes("/content/news-content/videos/assets/")) {
          try {
            const parsed = new URL(raw);
            return `${mediaBase.replace(/\/+$/, "")}${parsed.pathname}`;
          } catch (_) {
            return raw;
          }
        }
        return raw;
      }
      const normalized = raw.startsWith("/") ? raw : `/${raw}`;
      return `${mediaBase.replace(/\/+$/, "")}${normalized}`;
    };

    const format = String(req.body?.format || "landscape").toLowerCase();
    const resolution = String(req.body?.resolution || "").toLowerCase();
    const forceLandscape1080 = format === "landscape";
    const use720 = forceLandscape1080 ? false : resolution === "720p";
    const compositionId =
      format === "short"
        ? use720
          ? "NewsContentVideoShort720"
          : "NewsContentVideoShort"
        : use720
          ? "NewsContentVideoLandscape720"
          : "NewsContentVideoLandscape";
    const compositionFps = use720 ? 24 : 30;
    const requestedDurationInFrames = Number(req.body?.durationInFrames || 1);
    const requestedRenderFrameEnd = Number(req.body?.renderFrameEnd);
    const requestedApproach = String(req.body?.approach || "").trim();
    const filterByApproach = (clip) => {
      const approach = String(clip?.approach || "multi_sentence");
      return requestedApproach ? approach === requestedApproach : true;
    };
    const incomingScript = String(req.body?.script || "").trim();
    const renderScript = incomingScript || String(video.script || "");
    const incomingAudioUrl = String(req.body?.audioUrl || "").trim();
    const renderAudioUrl = incomingAudioUrl || String(video.audio_url || "");
    const resolvedAudioPath = resolveAudioPath(renderAudioUrl);
    const audioDurationSec = await getAudioDurationSec(resolvedAudioPath);
    const incomingSentences = Array.isArray(req.body?.sentences)
      ? req.body.sentences.map((s) => String(s || "").trim()).filter(Boolean)
      : [];
    const sentences = incomingSentences.length ? incomingSentences : splitSentences(renderScript);
    const computedDurationInFrames =
      requestedDurationInFrames > 1
        ? Math.max(1, requestedDurationInFrames)
        : audioDurationSec > 0
          ? Math.max(1, Math.round(audioDurationSec * compositionFps))
          : 1;
    const sentenceCount = Math.max(1, sentences.length);
    const incomingSentenceFrames = Array.isArray(req.body?.sentenceFrames)
      ? req.body.sentenceFrames.map((val) => Math.max(1, Math.round(Number(val || 1)))).filter(Number.isFinite)
      : [];
    const sentenceFrames =
      incomingSentenceFrames.length === sentenceCount
        ? incomingSentenceFrames
        : buildSentenceFrames(sentences, computedDurationInFrames);
    const computedPerSentenceSec =
      Number(req.body?.perSentenceSec || 0) > 0
        ? Number(req.body?.perSentenceSec)
        : audioDurationSec > 0
          ? audioDurationSec / sentenceCount
          : 5;
    const sourceClips = Array.isArray(req.body?.clips) && req.body.clips.length ? req.body.clips : video.clips;

    const props = {
      title: String(req.body?.title || "R4D News"),
      script: renderScript,
      audioUrl: toAbsolute(renderAudioUrl),
      clips: Array.isArray(sourceClips)
        ? sourceClips.filter(filterByApproach).map((clip) => ({
            ...clip,
            images: Array.isArray(clip?.images)
              ? clip.images.map((img) => ({
                  ...img,
                  url: toAbsolute(img?.url),
                }))
              : [],
          }))
        : [],
      sentences,
      sentenceFrames,
      perSentenceSec: computedPerSentenceSec,
      showBackgroundVideo: Boolean(req.body?.showBackgroundVideo !== false),
      stylePreset: "data",
      inheritClipTransitions: Boolean(req.body?.inheritClipTransitions !== false),
      transitionMode: String(req.body?.transitionMode || "single"),
      transitionSet: Array.isArray(req.body?.transitionSet) ? req.body.transitionSet : undefined,
      imagesPerTransition: Number(req.body?.imagesPerTransition || 5),
      secondsPerTransition: Number(req.body?.secondsPerTransition || 30),
      minTransitionDurationSec: Number(req.body?.minTransitionDurationSec || 1),
      skipShortTransitions: Boolean(req.body?.skipShortTransitions !== false),
      shortTransitionFallback: String(req.body?.shortTransitionFallback || "fade"),
      mediaOverlays: Array.isArray(req.body?.mediaOverlays)
        ? req.body.mediaOverlays.map((item) => ({
            type: String(item?.type || "image").toLowerCase() === "video" ? "video" : "image",
            url: toAbsolute(String(item?.url || "")),
            position: String(item?.position || "bottom-right"),
            size: String(item?.size || "md"),
            startSec: Math.max(0, Number(item?.startSec || 0)),
            endSec: Math.max(0.01, Number(item?.endSec || 0.01)),
            animation: String(item?.animation || "none").toLowerCase() === "fade" ? "fade" : "none",
            scheduleMode:
              String(item?.scheduleMode || "").toLowerCase() === "timeline_list"
                ? "timeline_list"
                : String(item?.scheduleMode || "").toLowerCase() === "random"
                  ? "random"
                  : String(item?.scheduleMode || "").toLowerCase() === "interval" || Boolean(item?.repeat)
                    ? "interval"
                    : "single",
            repeat: Boolean(item?.repeat),
            repeatEverySec: Math.max(1, Number(item?.repeatEverySec || 30)),
            timelineStarts: String(item?.timelineStarts || ""),
            randomCount: Math.max(1, Number(item?.randomCount || 3)),
            randomDurationSec: Math.max(0.5, Number(item?.randomDurationSec || 3)),
            label: String(item?.label || ""),
          }))
        : [],
      timeline:
        req.body?.timeline && typeof req.body.timeline === "object"
          ? {
              segments: Array.isArray(req.body.timeline?.segments)
                ? req.body.timeline.segments
                    .map((segment) => ({
                      ...segment,
                      path: toAbsolute(String(segment?.path || segment?.url || "")),
                    }))
                    .filter((segment) => Boolean(segment?.path))
                : [],
              overlays: Array.isArray(req.body.timeline?.overlays)
                ? req.body.timeline.overlays.map((overlay) => ({
                    startSec: Math.max(0, Number(overlay?.startSec || 0)),
                    endSec: Math.max(0.01, Number(overlay?.endSec || 0.01)),
                    text: String(overlay?.text || ""),
                    showFooter: Boolean(overlay?.showFooter),
                  }))
                : [],
              totalFrames: Math.max(
                1,
                Math.round(Number(req.body.timeline?.totalFrames || computedDurationInFrames || 1)),
              ),
            }
          : undefined,
    };

    const job = await videoRenderService.createNewsContentRenderJob({
      props,
      durationInFrames: computedDurationInFrames,
      renderFrameEnd: Number.isFinite(requestedRenderFrameEnd)
        ? Math.max(0, Math.min(Math.max(1, computedDurationInFrames) - 1, Math.floor(requestedRenderFrameEnd)))
        : undefined,
      compositionId,
      qualityMode: String(req.body?.qualityMode || "high"),
      useGpu: Boolean(req.body?.useGpu),
    });

    const mediaUrl = `content/news-content/videos/rendered/${job.fileName}`;
    await service.updateRenderInfo({
      userId: Number(req.user?.id || 0),
      id,
      renderJobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      status: "rendering",
    });

    return response(res, 202, "Render job created", {
      jobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      estimatedRenderSeconds: job.estimatedRenderSeconds,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to render video");
  }
}





async function generateFfmpegVideo(req, res) {
  try {
    const videoId = Number(req.body?.videoId || 0);
    if (!videoId) return response(res, 400, "videoId is required");
    const video = await service.getVideo({ userId: Number(req.user?.id || 0), id: videoId });
    if (!video) return response(res, 404, "Video not found");

    const format = String(req.body?.format || "landscape").toLowerCase();
    const resolution = String(req.body?.resolution || "1080p").toLowerCase();
    const fps = Math.max(12, Number(req.body?.fps || 30));
    const width = format === "short" ? (resolution === "720p" ? 720 : 1080) : (resolution === "720p" ? 1280 : 1920);
    const height = format === "short" ? (resolution === "720p" ? 1280 : 1920) : (resolution === "720p" ? 720 : 1080);

    const incomingTimeline = req.body?.timeline;
    if (!Array.isArray(incomingTimeline?.segments) || !incomingTimeline.segments.length) {
      return response(res, 400, "timeline.segments is required");
    }

    const minSegmentSec = 1 / fps;
    const segments = incomingTimeline.segments.map((segment) => {
      const durationSec = Math.max(minSegmentSec, Number(segment?.durationSec || minSegmentSec));
      const rawPath = String(segment?.path || segment?.url || "");
      return {
        path: resolveAssetPath(rawPath),
        durationSec,
        transition: "none",
      };
    });

    const overlays = Array.isArray(incomingTimeline?.overlays)
      ? incomingTimeline.overlays.map((overlay) => ({
          startSec: Math.max(0, Number(overlay?.startSec || 0)),
          endSec: Math.max(0.01, Number(overlay?.endSec || 0.01)),
          text: String(overlay?.text || ""),
          showFooter: Boolean(overlay?.showFooter),
        }))
      : [];

    const mediaOverlays = Array.isArray(req.body?.mediaOverlays)
      ? req.body.mediaOverlays.map((item) => ({
          type: String(item?.type || "image").toLowerCase() === "video" ? "video" : "image",
          url: resolveAssetPath(String(item?.url || "")),
          position: String(item?.position || "bottom-right"),
          size: String(item?.size || "md"),
          startSec: Math.max(0, Number(item?.startSec || 0)),
          endSec: Math.max(0.01, Number(item?.endSec || 0.01)),
          animation: String(item?.animation || "none").toLowerCase() === "fade" ? "fade" : "none",
          scheduleMode:
            String(item?.scheduleMode || "").toLowerCase() === "timeline_list"
              ? "timeline_list"
              : String(item?.scheduleMode || "").toLowerCase() === "random"
                ? "random"
                : String(item?.scheduleMode || "").toLowerCase() === "interval" || Boolean(item?.repeat)
                  ? "interval"
                  : "single",
          repeat: Boolean(item?.repeat),
          repeatEverySec: Math.max(1, Number(item?.repeatEverySec || 30)),
          timelineStarts: String(item?.timelineStarts || ""),
          randomCount: Math.max(1, Number(item?.randomCount || 3)),
          randomDurationSec: Math.max(0.5, Number(item?.randomDurationSec || 3)),
          label: String(item?.label || ""),
        }))
      : [];

    const requestedAudioUrl = String(req.body?.audioUrl || "").trim();
    const audioPath = resolveAudioPath(requestedAudioUrl || String(video.audio_url || ""));
    const audioDurationSec = await getAudioDurationSec(audioPath);
    if (!audioDurationSec || audioDurationSec <= 0) {
      return response(res, 400, "Audio is required to generate synced video");
    }

    const job = await videoRenderService.createNewsContentFfmpegTimelineJob({
      segments,
      overlays,
      mediaOverlays,
      audioPath,
      width,
      height,
      fps,
      qualityMode: String(req.body?.qualityMode || "high"),
      meta: {
        renderMode: "generate_video_timeline",
        audioPreview: audioPath,
        requestedAudioUrl: requestedAudioUrl || String(video.audio_url || ""),
        segmentsCount: segments.length,
        overlaysCount: overlays.length,
        mediaOverlaysCount: mediaOverlays.length,
        timelineProvided: true,
      },
    });

    const mediaUrl = `content/news-content/videos/rendered/${job.fileName}`;
    await service.updateRenderInfo({
      userId: Number(req.user?.id || 0),
      id: videoId,
      renderJobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      status: "rendering",
    });

    return response(res, 202, "FFmpeg video render job created", {
      jobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      estimatedRenderSeconds: job.estimatedRenderSeconds,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate FFmpeg video");
  }
}

async function generateRemotionPreviewVideo(req, res) {
  try {
    const videoId = Number(req.body?.videoId || 0);
    if (!videoId) return response(res, 400, "videoId is required");
    const video = await service.getVideo({ userId: Number(req.user?.id || 0), id: videoId });
    if (!video) return response(res, 404, "Video not found");

    const forwardedProto = req.headers["x-forwarded-proto"];
    const forwardedHost = req.headers["x-forwarded-host"];
    const proto = Array.isArray(forwardedProto) ? forwardedProto[0] : forwardedProto || req.protocol;
    const host = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost || req.get("host");
    const requestBase = proto && host ? `${proto}://${host}` : "";

    const format = String(req.body?.format || "landscape").toLowerCase();
    const resolution = String(req.body?.resolution || "1080p").toLowerCase();
    const use720 = resolution === "720p";
    const compositionId =
      format === "short"
        ? use720
          ? "NewsContentVideoShort720"
          : "NewsContentVideoShort"
        : use720
          ? "NewsContentVideoLandscape720"
          : "NewsContentVideoLandscape";

    const durationInFrames = Math.max(1, Number(req.body?.durationInFrames || 1));
    const renderFrameEndRaw = Number(req.body?.renderFrameEnd);
    const renderFrameEnd = Number.isFinite(renderFrameEndRaw)
      ? Math.max(0, Math.min(durationInFrames - 1, Math.floor(renderFrameEndRaw)))
      : undefined;

    const incomingPreviewProps =
      req.body?.previewProps && typeof req.body.previewProps === "object"
        ? req.body.previewProps
        : null;

    const script = String(incomingPreviewProps?.script || req.body?.script || video?.script || "").trim();
    if (!script) return response(res, 400, "script is required");

    const toAbsoluteUrl = (input) => {
      const raw = String(input || "").trim();
      if (!raw) return "";
      if (raw.startsWith("http://") || raw.startsWith("https://") || raw.startsWith("data:")) return raw;
      if (!requestBase) return raw;
      return `${requestBase.replace(/\/+$/, "")}/${raw.replace(/^\/+/, "")}`;
    };

    const toAudioSource = (input) => {
      const raw = String(input || "").trim();
      if (!raw) return "";
      if (raw.startsWith("data:")) return raw;
      const local = resolveAudioPath(raw);
      if (local && fs.existsSync(local)) {
        return audioFileToDataUrl(local) || pathToFileURL(local).href;
      }
      return toAbsoluteUrl(raw);
    };

    const toImageSource = (input) => {
      const raw = String(input || "").trim();
      if (!raw) return "";
      if (raw.startsWith("data:")) return raw;
      const local = resolveAssetPath(raw);
      if (local && fs.existsSync(local)) {
        return imageFileToDataUrl(local) || pathToFileURL(local).href;
      }
      return toAbsoluteUrl(raw);
    };

    const audioUrl = toAudioSource(String(incomingPreviewProps?.audioUrl || req.body?.audioUrl || video?.audio_url || ""));
    if (!audioUrl) return response(res, 400, "audioUrl is required");

    const incomingSentences = Array.isArray(incomingPreviewProps?.sentences)
      ? incomingPreviewProps.sentences.map((s) => String(s || "").trim()).filter(Boolean)
      : Array.isArray(req.body?.sentences)
        ? req.body.sentences.map((s) => String(s || "").trim()).filter(Boolean)
      : [];
    const sentences = incomingSentences.length ? incomingSentences : splitSentences(script);
    const sentenceFrames = Array.isArray(incomingPreviewProps?.sentenceFrames)
      ? incomingPreviewProps.sentenceFrames.map((val) => Math.max(1, Math.round(Number(val || 1)))).filter(Number.isFinite)
      : Array.isArray(req.body?.sentenceFrames)
        ? req.body.sentenceFrames.map((val) => Math.max(1, Math.round(Number(val || 1)))).filter(Number.isFinite)
      : buildSentenceFrames(sentences, durationInFrames);
    const sourceClips = Array.isArray(incomingPreviewProps?.clips)
      ? incomingPreviewProps.clips
      : Array.isArray(req.body?.clips)
        ? req.body.clips
        : Array.isArray(video?.clips)
          ? video.clips
          : [];

    const props = {
      title: String(incomingPreviewProps?.title || req.body?.title || "R4D News"),
      script,
      audioUrl,
      clips: sourceClips.map((clip) => ({
        ...clip,
        images: Array.isArray(clip?.images)
          ? clip.images.map((img) => ({
              ...img,
              url: toImageSource(img?.url),
            }))
          : [],
      })),
      sentences,
      sentenceFrames,
      perSentenceSec: Number(incomingPreviewProps?.perSentenceSec || req.body?.perSentenceSec || 5),
      showBackgroundVideo: Boolean(
        incomingPreviewProps?.showBackgroundVideo !== undefined
          ? incomingPreviewProps?.showBackgroundVideo
          : req.body?.showBackgroundVideo !== false
      ),
      stylePreset: String(incomingPreviewProps?.stylePreset || "data"),
      inheritClipTransitions: Boolean(
        incomingPreviewProps?.inheritClipTransitions !== undefined
          ? incomingPreviewProps?.inheritClipTransitions
          : req.body?.inheritClipTransitions !== false
      ),
      transitionMode: String(incomingPreviewProps?.transitionMode || req.body?.transitionMode || "single"),
      transitionSet: Array.isArray(incomingPreviewProps?.transitionSet)
        ? incomingPreviewProps.transitionSet
        : Array.isArray(req.body?.transitionSet)
          ? req.body.transitionSet
          : undefined,
      imagesPerTransition: Number(incomingPreviewProps?.imagesPerTransition || req.body?.imagesPerTransition || 5),
      secondsPerTransition: Number(incomingPreviewProps?.secondsPerTransition || req.body?.secondsPerTransition || 30),
      minTransitionDurationSec: Number(
        incomingPreviewProps?.minTransitionDurationSec || req.body?.minTransitionDurationSec || 1
      ),
      skipShortTransitions: Boolean(
        incomingPreviewProps?.skipShortTransitions !== undefined
          ? incomingPreviewProps?.skipShortTransitions
          : req.body?.skipShortTransitions !== false
      ),
      shortTransitionFallback: String(
        incomingPreviewProps?.shortTransitionFallback || req.body?.shortTransitionFallback || "fade"
      ),
      mediaOverlays: Array.isArray(incomingPreviewProps?.mediaOverlays)
        ? incomingPreviewProps.mediaOverlays.map((item) => ({
            ...item,
            url: toAbsoluteUrl(String(item?.url || "")),
          }))
        : Array.isArray(req.body?.mediaOverlays)
          ? req.body.mediaOverlays.map((item) => ({
              ...item,
              url: toAbsoluteUrl(String(item?.url || "")),
            }))
        : [],
      timeline:
        (incomingPreviewProps?.timeline && typeof incomingPreviewProps.timeline === "object") ||
        (req.body?.timeline && typeof req.body.timeline === "object")
          ? {
              segments: Array.isArray((incomingPreviewProps?.timeline || req.body?.timeline)?.segments)
                ? (incomingPreviewProps?.timeline || req.body?.timeline).segments
                    .map((segment) => ({
                      ...segment,
                      path: toImageSource(String(segment?.path || segment?.url || "")),
                    }))
                    .filter((segment) => Boolean(segment?.path))
                : [],
              overlays: Array.isArray((incomingPreviewProps?.timeline || req.body?.timeline)?.overlays)
                ? (incomingPreviewProps?.timeline || req.body?.timeline).overlays.map((overlay) => ({
                    startSec: Math.max(0, Number(overlay?.startSec || 0)),
                    endSec: Math.max(0.01, Number(overlay?.endSec || 0.01)),
                    text: String(overlay?.text || ""),
                    showFooter: Boolean(overlay?.showFooter),
                  }))
                : [],
              totalFrames: Math.max(
                1,
                Math.round(
                  Number(
                    (incomingPreviewProps?.timeline || req.body?.timeline)?.totalFrames ||
                      durationInFrames ||
                      1,
                  ),
                ),
              ),
            }
          : undefined,
    };

    const job = await remotionPreviewRenderService.createRemotionPreviewJob({
      props,
      compositionId,
      durationInFrames,
      renderFrameEnd,
      qualityMode: String(req.body?.qualityMode || "high"),
      useGpu: Boolean(req.body?.useGpu),
    });

    const mediaUrl = `content/news-content/videos/rendered/${job.fileName}`;
    await service.updateRenderInfo({
      userId: Number(req.user?.id || 0),
      id: videoId,
      renderJobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      status: "rendering",
    });

    return response(res, 202, "Remotion preview render job created", {
      jobId: job.jobId,
      fileName: job.fileName,
      videoUrl: mediaUrl,
      estimatedRenderSeconds: job.estimatedRenderSeconds,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate Remotion preview video");
  }
}

async function renderStatusRemotionPreview(req, res) {
  try {
    const id = Number(req.params?.id || 0);
    const jobId = String(req.params?.jobId || "");
    const job = remotionPreviewRenderService.getRemotionPreviewJob(jobId);
    if (job.status === "completed") {
      await service.updateRenderInfo({
        userId: Number(req.user?.id || 0),
        id,
        status: "ready_for_download",
      });
    }
    return response(res, 200, "Remotion preview render job status", job);
  } catch (err) {
    return response(res, 404, err?.message || "Render job not found");
  }
}

async function createFastGpuRenderJob(req, res) {
  try {
    const videoId = Number(req.body?.videoId || 0);
    if (!videoId) return response(res, 400, "videoId is required");
    const video = await service.getVideo({ userId: Number(req.user?.id || 0), id: videoId });
    if (!video) return response(res, 404, "Video not found");

    const forwardedProto = req.headers["x-forwarded-proto"];
    const forwardedHost = req.headers["x-forwarded-host"];
    const proto = Array.isArray(forwardedProto) ? forwardedProto[0] : forwardedProto || req.protocol;
    const host = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost || req.get("host");
    const requestBase = proto && host ? `${proto}://${host}` : "";

    const incomingPreviewProps =
      req.body?.previewProps && typeof req.body.previewProps === "object"
        ? req.body.previewProps
        : null;

    const script = String(incomingPreviewProps?.script || req.body?.script || video?.script || "").trim();
    if (!script) return response(res, 400, "script is required");

    const toAbsoluteUrl = (input) => {
      const raw = String(input || "").trim();
      if (!raw) return "";
      if (raw.startsWith("http://") || raw.startsWith("https://") || raw.startsWith("data:")) return raw;
      if (!requestBase) return raw;
      return `${requestBase.replace(/\/+$/, "")}/${raw.replace(/^\/+/, "")}`;
    };

    const audioUrl = String(incomingPreviewProps?.audioUrl || req.body?.audioUrl || video?.audio_url || "").trim();
    if (!audioUrl) return response(res, 400, "audioUrl is required");

    const incomingSentences = Array.isArray(incomingPreviewProps?.sentences)
      ? incomingPreviewProps.sentences.map((s) => String(s || "").trim()).filter(Boolean)
      : Array.isArray(req.body?.sentences)
        ? req.body.sentences.map((s) => String(s || "").trim()).filter(Boolean)
        : [];
    const durationInFrames = Math.max(1, Number(req.body?.durationInFrames || incomingPreviewProps?.timeline?.totalFrames || 1));
    const sentences = incomingSentences.length ? incomingSentences : splitSentences(script);
    const sentenceFrames = Array.isArray(incomingPreviewProps?.sentenceFrames)
      ? incomingPreviewProps.sentenceFrames.map((val) => Math.max(1, Math.round(Number(val || 1)))).filter(Number.isFinite)
      : buildSentenceFrames(sentences, durationInFrames);
    const sourceClips = Array.isArray(incomingPreviewProps?.clips)
      ? incomingPreviewProps.clips
      : Array.isArray(req.body?.clips)
        ? req.body.clips
        : Array.isArray(video?.clips)
          ? video.clips
          : [];

    const timelineSource =
      (incomingPreviewProps?.timeline && typeof incomingPreviewProps.timeline === "object")
        ? incomingPreviewProps.timeline
        : req.body?.timeline && typeof req.body.timeline === "object"
          ? req.body.timeline
          : null;

    const props = {
      title: String(incomingPreviewProps?.title || req.body?.title || video?.title || "R4D News"),
      script,
      audioUrl: toAbsoluteUrl(audioUrl),
      clips: sourceClips.map((clip) => ({
        ...clip,
        images: Array.isArray(clip?.images)
          ? clip.images.map((img) => ({
              ...img,
              url: toAbsoluteUrl(String(img?.url || "")),
            }))
          : [],
      })),
      sentences,
      sentenceFrames,
      perSentenceSec: Number(incomingPreviewProps?.perSentenceSec || req.body?.perSentenceSec || 5),
      showBackgroundVideo: Boolean(
        incomingPreviewProps?.showBackgroundVideo !== undefined
          ? incomingPreviewProps?.showBackgroundVideo
          : req.body?.showBackgroundVideo !== false,
      ),
      stylePreset: String(incomingPreviewProps?.stylePreset || "data"),
      inheritClipTransitions: Boolean(
        incomingPreviewProps?.inheritClipTransitions !== undefined
          ? incomingPreviewProps?.inheritClipTransitions
          : req.body?.inheritClipTransitions !== false,
      ),
      transitionMode: String(incomingPreviewProps?.transitionMode || req.body?.transitionMode || "single"),
      transitionSet: Array.isArray(incomingPreviewProps?.transitionSet)
        ? incomingPreviewProps.transitionSet
        : Array.isArray(req.body?.transitionSet)
          ? req.body.transitionSet
          : undefined,
      imagesPerTransition: Number(incomingPreviewProps?.imagesPerTransition || req.body?.imagesPerTransition || 5),
      secondsPerTransition: Number(incomingPreviewProps?.secondsPerTransition || req.body?.secondsPerTransition || 30),
      minTransitionDurationSec: Number(
        incomingPreviewProps?.minTransitionDurationSec || req.body?.minTransitionDurationSec || 1,
      ),
      skipShortTransitions: Boolean(
        incomingPreviewProps?.skipShortTransitions !== undefined
          ? incomingPreviewProps?.skipShortTransitions
          : req.body?.skipShortTransitions !== false,
      ),
      shortTransitionFallback: String(
        incomingPreviewProps?.shortTransitionFallback || req.body?.shortTransitionFallback || "fade",
      ),
      mediaOverlays: Array.isArray(incomingPreviewProps?.mediaOverlays)
        ? incomingPreviewProps.mediaOverlays.map((item) => ({
            ...item,
            url: toAbsoluteUrl(String(item?.url || "")),
          }))
        : Array.isArray(req.body?.mediaOverlays)
          ? req.body.mediaOverlays.map((item) => ({
              ...item,
              url: toAbsoluteUrl(String(item?.url || "")),
            }))
          : [],
      timeline: timelineSource
        ? {
            segments: Array.isArray(timelineSource?.segments)
              ? timelineSource.segments
                  .map((segment) => ({
                    ...segment,
                    path: toAbsoluteUrl(String(segment?.path || segment?.url || "")),
                  }))
                  .filter((segment) => Boolean(segment?.path))
              : [],
            overlays: Array.isArray(timelineSource?.overlays)
              ? timelineSource.overlays.map((overlay) => ({
                  startSec: Math.max(0, Number(overlay?.startSec || 0)),
                  endSec: Math.max(0.01, Number(overlay?.endSec || 0.01)),
                  text: String(overlay?.text || ""),
                  showFooter: Boolean(overlay?.showFooter),
                }))
              : [],
            totalFrames: Math.max(1, Math.round(Number(timelineSource?.totalFrames || durationInFrames || 1))),
          }
        : undefined,
    };

    const format = String(req.body?.format || "landscape").toLowerCase();
    const resolution = String(req.body?.resolution || "1080p").toLowerCase();
    const use720 = resolution === "720p";
    const compositionId =
      format === "short"
        ? use720
          ? "NewsContentVideoShort720"
          : "NewsContentVideoShort"
        : use720
          ? "NewsContentVideoLandscape720"
          : "NewsContentVideoLandscape";

    const renderFrameEndRaw = Number(req.body?.renderFrameEnd);
    const renderFrameEnd = Number.isFinite(renderFrameEndRaw)
      ? Math.max(0, Math.min(durationInFrames - 1, Math.floor(renderFrameEndRaw)))
      : undefined;

    const job = await gpuFastRenderV2Service.createFastGpuJob({
      userId: Number(req.user?.id || 0),
      videoId,
      title: String(props.title || "R4D News"),
      props,
      compositionId,
      durationInFrames,
      renderFrameEnd,
      qualityMode: String(req.body?.qualityMode || "gpu"),
    });

    await service.updateRenderInfo({
      userId: Number(req.user?.id || 0),
      id: videoId,
      renderJobId: job.jobId,
      fileName: job.fileName,
      videoUrl: job.videoUrl,
      status: "rendering",
    });

    return response(res, 202, "Fast GPU render job created", job);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to create fast GPU render job");
  }
}

async function getFastGpuRenderJob(req, res) {
  try {
    const jobId = String(req.params?.jobId || "");
    const data = gpuFastRenderV2Service.getFastGpuJob(jobId);
    if (Number(data?.userId || 0) !== Number(req.user?.id || 0)) {
      return response(res, 404, "Render job not found");
    }
    return response(res, 200, "Fast GPU render job status", data);
  } catch (err) {
    return response(res, 404, err?.message || "Render job not found");
  }
}

async function listFastGpuRenderJobs(req, res) {
  try {
    const inProgressOnly = String(req.query?.scope || "").toLowerCase() === "in_progress";
    const data = gpuFastRenderV2Service.listFastGpuJobs({
      userId: Number(req.user?.id || 0),
      inProgressOnly,
    });
    return response(res, 200, "Fast GPU render jobs", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to list fast GPU render jobs");
  }
}


async function renderStatus(req, res) {
  try {
    const id = Number(req.params?.id || 0);
    const jobId = String(req.params?.jobId || "");
    const job = videoRenderService.getRenderJob(jobId);
    if (job.status === "completed") {
      await service.updateRenderInfo({
        userId: Number(req.user?.id || 0),
        id,
        status: "ready_for_download",
      });
    }
    return response(res, 200, "Render job status", job);
  } catch (err) {
    return response(res, 404, err?.message || "Render job not found");
  }
}


async function streamRenderedVideo(req, res) {
  try {
    const filePath = videoRenderService.getVideoPath(req.params?.fileName);
    if (!fs.existsSync(filePath)) {
      return response(res, 404, "Video file not found");
    }
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to read video file");
  }
}

async function streamImage(req, res) {
  try {
    const filePath = service.getImagePath(req.params?.fileName);
    if (!fs.existsSync(filePath)) return response(res, 404, "Image not found");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to read image");
  }
}

async function streamAudio(req, res) {
  try {
    const filePath = service.getAudioPath(req.params?.fileName);
    if (!fs.existsSync(filePath)) return response(res, 404, "Audio not found");
    res.setHeader("Content-Type", "audio/wav");
    res.setHeader("Cache-Control", "no-store");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to read audio");
  }
}

async function generateImageKeyword(req, res) {
  try {
    const sentence = String(req.body?.sentence || "").trim();
    if (!sentence) return response(res, 400, "sentence is required");
    const prompt =
      "You are an expert image search assistant. Return a short English phrase (3-6 words) that best describes an image to represent the sentence below. Respond with only the phrase, no punctuation or quotes.\n\nSentence:\n" +
      sentence;
    const result = await chatWithOllama({ prompt });
    const keyword = String(result?.response || "").trim();
    return response(res, 200, "Keyword generated", { keyword });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to generate keyword");
  }
}

module.exports = {
  listVideos,
  createVideo,
  getVideo,
  updateVideo,
  renderVideo,
  generateFfmpegVideo,
  generateRemotionPreviewVideo,
  createFastGpuRenderJob,
  getFastGpuRenderJob,
  listFastGpuRenderJobs,
  renderStatus,
  renderStatusRemotionPreview,
  generateImageKeyword,
  streamRenderedVideo,
  uploadImage,
  uploadAudio,
  streamImage,
  streamAudio,
};
