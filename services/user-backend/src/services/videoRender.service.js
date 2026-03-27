const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { pathToFileURL } = require("url");

const RENDER_JOBS = new Map();
const GENERATED_DIR = path.join(__dirname, "..", "..", "generated-video");
const GENERATED_ASSETS_DIR = path.join(GENERATED_DIR, "assets");
const GENERATED_JOBS_DIR = path.join(GENERATED_DIR, "jobs");
const GENERATED_AUDIO_DIR = path.join(__dirname, "..", "..", "generated-audio");
const FRONTEND_ROOT = process.env.REMOTION_PROJECT_ROOT
  ? path.resolve(process.env.REMOTION_PROJECT_ROOT)
  : path.resolve(__dirname, "..", "..", "..", "..", "..", "run4dream_frontend");
const REMOTION_ENTRY = "src/remotion/index.ts";
const REMOTION_COMPOSITION_ID = "SceneStoryVideo";
const VIDEO_FPS = 30;
const PROGRESS_SAVE_STEP = 2; // persist at least each +2%
const PROGRESS_SAVE_MIN_MS = 1500; // and not more often than this interval
const QUALITY_PRESETS = {
  draft: { crf: 30, preset: "veryfast", speedFactor: 1.2 },
  standard: { crf: 23, preset: "medium", speedFactor: 2.2 },
  high: { crf: 18, preset: "slow", speedFactor: 3.4 },
};
const RENDER_MEDIA_BASE_URL = process.env.RENDER_MEDIA_BASE_URL
  || `http://127.0.0.1:${Number(process.env.PORT || 8001)}`;

function ensureDirs() {
  fs.mkdirSync(GENERATED_DIR, { recursive: true });
  fs.mkdirSync(GENERATED_ASSETS_DIR, { recursive: true });
  fs.mkdirSync(GENERATED_JOBS_DIR, { recursive: true });
}

function getJobFilePath(jobId) {
  const safe = path.basename(String(jobId || ""));
  if (!safe || safe !== jobId) throw new Error("Invalid render job id");
  return path.join(GENERATED_JOBS_DIR, `${safe}.json`);
}

function saveJobState(job) {
  try {
    ensureDirs();
    const p = getJobFilePath(job.id);
    const tmp = `${p}.tmp`;
    const payload = JSON.stringify(job, null, 2);

    // Atomic-ish write on Windows: write temp then rename.
    for (let i = 0; i < 3; i += 1) {
      try {
        fs.writeFileSync(tmp, payload, "utf8");
        fs.renameSync(tmp, p);
        return true;
      } catch (_) {
        // short busy-wait retry to handle transient file lock/AV indexing
      }
    }

    // Final fallback direct write.
    fs.writeFileSync(p, payload, "utf8");
    return true;
  } catch (err) {
    // Never crash render loop because of status persistence issues.
    console.error("Failed to persist render job state:", err?.message || err);
    return false;
  }
}

function readJobState(jobId) {
  try {
    const p = getJobFilePath(jobId);
    if (!fs.existsSync(p)) return null;
    const raw = fs.readFileSync(p, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("Failed to read render job state:", err?.message || err);
    return null;
  }
}

function safeFileName(name) {
  const raw = String(name || "").trim();
  return raw.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function parseDataUrl(dataUrl) {
  const raw = String(dataUrl || "");
  const match = raw.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$/);
  if (!match) throw new Error("Invalid image dataUrl");
  return { mime: match[1], base64: match[2] };
}

function mimeToExtension(mime) {
  const m = String(mime || "").toLowerCase();
  if (m === "image/jpeg" || m === "image/jpg") return ".jpg";
  if (m === "image/png") return ".png";
  if (m === "image/webp") return ".webp";
  return ".png";
}

function saveSceneImage({ fileName, dataUrl }) {
  ensureDirs();
  const { mime, base64 } = parseDataUrl(dataUrl);
  const ext = mimeToExtension(mime);
  const rawName = safeFileName(fileName || "scene-image");
  const cleanName = rawName.replace(/\.[a-zA-Z0-9]{1,10}$/, "") || "scene-image";
  const finalName = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}-${cleanName}${ext}`;
  const outPath = path.join(GENERATED_ASSETS_DIR, finalName);
  const binary = Buffer.from(base64, "base64");
  fs.writeFileSync(outPath, binary);
  return {
    fileName: finalName,
    filePath: outPath,
  };
}

function getAssetPath(fileName) {
  const safe = path.basename(fileName || "");
  if (!safe || safe !== fileName) throw new Error("Invalid asset file name");
  return path.join(GENERATED_ASSETS_DIR, safe);
}

function getVideoPath(fileName) {
  const safe = path.basename(fileName || "");
  if (!safe || safe !== fileName) throw new Error("Invalid video file name");
  return path.join(GENERATED_DIR, safe);
}

function getRemotionCliCmd() {
  const frontendCli = path.join(FRONTEND_ROOT, "node_modules", "@remotion", "cli", "remotion-cli.js");
  if (fs.existsSync(frontendCli)) {
    return { cmd: process.execPath, argsPrefix: [frontendCli] };
  }

  const backendCli = path.join(__dirname, "..", "..", "node_modules", "@remotion", "cli", "remotion-cli.js");
  if (fs.existsSync(backendCli)) {
    return { cmd: process.execPath, argsPrefix: [backendCli] };
  }

  throw new Error(
    "Remotion CLI not found. Install @remotion/cli in frontend or backend before rendering.",
  );
}

function extractProgressPercent(text, totalFrames) {
  const raw = stripAnsi(String(text || ""));
  const staged = extractStageProgress(raw, totalFrames);
  if (staged !== null) return clampNumber(staged, 0, 99, 0);

  const direct = raw.match(/(\d{1,3})%/);
  if (direct?.[1]) {
    return clampNumber(Number(direct[1]), 0, 99, 0);
  }

  const frameProgress = raw.match(/(\d+)\s*\/\s*(\d+)/);
  if (frameProgress?.[1] && frameProgress?.[2]) {
    const done = Number(frameProgress[1]);
    const total = Number(frameProgress[2]) || totalFrames || 1;
    return clampNumber((done / total) * 100, 0, 99, 0);
  }

  return null;
}

function extractStageProgress(rawText, totalFrames) {
  const text = String(rawText || "");
  const total = Math.max(1, Number(totalFrames || 1));

  // Stage 2/3: Rendering frames (Nx) x/total
  const rendering = text.match(/Rendering frames[^0-9]*(\d+)\s*\/\s*(\d+)/i);
  if (rendering?.[1] && rendering?.[2]) {
    const done = Number(rendering[1]);
    const den = Number(rendering[2]) || total;
    const ratio = clampNumber((done / Math.max(1, den)) * 100, 0, 100, 0);
    // Rendering stage consumes roughly first 75%
    return (ratio * 0.75);
  }

  // Stage 3/3: Encoding video x/total
  const encoding = text.match(/Encoding video[^0-9]*(\d+)\s*\/\s*(\d+)/i);
  if (encoding?.[1] && encoding?.[2]) {
    const done = Number(encoding[1]);
    const den = Number(encoding[2]) || total;
    const ratio = clampNumber((done / Math.max(1, den)) * 100, 0, 100, 0);
    // Encoding stage consumes final 25%
    return 75 + (ratio * 0.25);
  }

  return null;
}

function stripAnsi(text) {
  // eslint-disable-next-line no-control-regex
  return String(text || "").replace(/\u001b\[[0-9;?]*[ -/]*[@-~]/g, "");
}

function getQualityPreset(mode) {
  const key = String(mode || "standard").toLowerCase();
  return QUALITY_PRESETS[key] || QUALITY_PRESETS.standard;
}

function estimateRenderSeconds(totalFrames, qualityMode) {
  const preset = getQualityPreset(qualityMode);
  const seconds = totalFrames / VIDEO_FPS;
  return Math.max(30, Math.round(seconds * preset.speedFactor));
}

function shouldPersistProgress(state, nextProgress) {
  const prev = Number(state?.progress || 0);
  const now = Date.now();
  const last = Number(state?.lastProgressPersistAt || 0);
  const movedEnough = Math.abs(nextProgress - prev) >= PROGRESS_SAVE_STEP;
  const oldEnough = now - last >= PROGRESS_SAVE_MIN_MS;
  return movedEnough && oldEnough;
}

function projectProgressFromElapsed(job) {
  if (!job || job.status !== "rendering") return Number(job?.progress || 0);
  const lastRealtime = Number(job.lastRealtimeProgressAt || 0);
  if (lastRealtime > 0 && Date.now() - lastRealtime < 30000) {
    return Number(job.progress || 0);
  }
  const estimated = Number(job.estimatedRenderSeconds || 0);
  const createdAtMs = Number(new Date(job.createdAt || 0).getTime());
  if (!estimated || !createdAtMs) return Number(job.progress || 0);

  const elapsedSec = Math.max(0, (Date.now() - createdAtMs) / 1000);
  const projected = clampNumber((elapsedSec / estimated) * 100, 1, 95, 1);
  return Math.max(Number(job.progress || 0), projected);
}

function reconcileRenderingState(job) {
  if (!job || job.status !== "rendering") return job;
  if (Number(job.progress || 0) >= 100) {
    job.progress = 99;
    saveJobState(job);
  }

  // If output file already exists, render effectively completed (close event may have been missed).
  try {
    if (job.filePath && fs.existsSync(job.filePath)) {
      const stat = fs.statSync(job.filePath);
      if (stat.size > 0) {
        job.status = "completed";
        job.progress = 100;
        job.finishedAt = job.finishedAt || new Date().toISOString();
        saveJobState(job);
        return job;
      }
    }
  } catch (_) {
    // ignore fs errors and continue checks
  }

  // Timeout guard: if render is far beyond estimate, fail with actionable error.
  const estimated = Number(job.estimatedRenderSeconds || 0);
  const createdAtMs = Number(new Date(job.createdAt || 0).getTime());
  if (estimated > 0 && createdAtMs > 0) {
    const elapsedSec = Math.max(0, (Date.now() - createdAtMs) / 1000);
    if (Number(job.progress || 0) >= 99 && elapsedSec > estimated + 180) {
      job.status = "failed";
      job.error =
        job.error ||
        `Render stalled at finalization (99%) for too long (${Math.round(elapsedSec)}s)`;
      job.finishedAt = new Date().toISOString();
      saveJobState(job);
      return job;
    }
    const maxAllowed = Math.max(estimated * 4, estimated + 1200); // 4x estimate or +20 min
    if (elapsedSec > maxAllowed) {
      job.status = "failed";
      job.error =
        job.error ||
        `Render timeout: exceeded ${Math.round(maxAllowed)}s without completion`;
      job.finishedAt = new Date().toISOString();
      saveJobState(job);
      return job;
    }
  }

  // Process liveness check (works when backend process that started job is still up).
  if (job.renderPid) {
    try {
      process.kill(Number(job.renderPid), 0);
    } catch (_) {
      job.status = "failed";
      job.error =
        job.error || "Render process exited unexpectedly before output was produced";
      job.finishedAt = new Date().toISOString();
      job.renderPid = null;
      saveJobState(job);
      return job;
    }
  }

  return job;
}

function runRender(jobId, { props, durationInFrames, outputPath, qualityMode }) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(FRONTEND_ROOT)) {
      reject(new Error(`Remotion project root not found: ${FRONTEND_ROOT}`));
      return;
    }

    const propsFilePath = path.join(GENERATED_DIR, `render-props-${jobId}.json`);
    fs.writeFileSync(propsFilePath, JSON.stringify(props), "utf8");

    const preset = getQualityPreset(qualityMode);
    const cli = getRemotionCliCmd();
    const args = [
      ...cli.argsPrefix,
      "render",
      REMOTION_ENTRY,
      REMOTION_COMPOSITION_ID,
      outputPath,
      `--props=${propsFilePath}`,
      "--frames",
      `0-${Math.max(1, durationInFrames) - 1}`,
      "--codec",
      "h264",
      "--crf",
      String(preset.crf),
      "--x264-preset",
      String(preset.preset),
      "--overwrite",
    ];

    const child = spawn(cli.cmd, args, {
      cwd: FRONTEND_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    });
    const stateAtStart = RENDER_JOBS.get(jobId);
    if (stateAtStart) {
      stateAtStart.renderPid = Number(child.pid || 0) || null;
      stateAtStart.renderStartedAt = new Date().toISOString();
      saveJobState(stateAtStart);
    }
    const estimatedSecs = Number(stateAtStart?.estimatedRenderSeconds || 0);
    const startedAtMs = Date.now();
    const hardTimeoutMs = Math.max(
      10 * 60 * 1000,
      Math.round((estimatedSecs > 0 ? estimatedSecs : 300) * 3 * 1000),
    );
    const hardTimer = setTimeout(() => {
      try {
        child.kill("SIGTERM");
      } catch (_) {
        // ignore
      }
    }, hardTimeoutMs);

    const timer = setInterval(() => {
      const state = RENDER_JOBS.get(jobId);
      if (!state || state.status !== "rendering") return;
      if (estimatedSecs <= 0) return;

      const elapsed = (Date.now() - startedAtMs) / 1000;
      const projected = clampNumber((elapsed / estimatedSecs) * 100, 1, 99, 1);
      if ((!Number.isFinite(state.progress) || state.progress < projected) && shouldPersistProgress(state, projected)) {
        state.progress = projected;
        state.lastProgressPersistAt = Date.now();
        saveJobState(state);
      }
    }, 3000);

    let stderr = "";
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      const msg = chunk.toString();
      stdout += msg;
      const pct = extractProgressPercent(msg, durationInFrames);
      const state = RENDER_JOBS.get(jobId);
      if (state && pct !== null && shouldPersistProgress(state, pct)) {
        state.progress = pct;
        state.lastRealtimeProgressAt = Date.now();
        state.lastProgressPersistAt = Date.now();
        state.lastStdout = stdout.slice(-4000);
        saveJobState(state);
      }
    });
    child.stderr.on("data", (chunk) => {
      const msg = chunk.toString();
      stderr += msg;
      const pct = extractProgressPercent(msg, durationInFrames);
      const state = RENDER_JOBS.get(jobId);
      if (state && pct !== null && shouldPersistProgress(state, pct)) {
        state.progress = pct;
        state.lastRealtimeProgressAt = Date.now();
        state.lastProgressPersistAt = Date.now();
        state.lastStderr = stderr.slice(-4000);
        saveJobState(state);
      }
    });

    child.on("error", (err) => {
      clearInterval(timer);
      clearTimeout(hardTimer);
      const state = RENDER_JOBS.get(jobId);
      if (state) {
        state.renderPid = null;
        state.lastStdout = stdout.slice(-4000);
        state.lastStderr = stderr.slice(-4000);
        saveJobState(state);
      }
      reject(err);
    });
    child.on("close", (code, signal) => {
      clearInterval(timer);
      clearTimeout(hardTimer);
      try {
        if (fs.existsSync(propsFilePath)) fs.unlinkSync(propsFilePath);
      } catch (_) {
        // best-effort cleanup
      }
      if (code !== 0) {
        const details = stderr || stdout || `Render failed with code ${code}, signal ${signal || "none"}`;
        reject(new Error(details));
        return;
      }
      const state = RENDER_JOBS.get(jobId);
      if (state) {
        state.progress = 100;
        state.renderPid = null;
        state.lastStdout = stdout.slice(-4000);
        state.lastStderr = stderr.slice(-4000);
        state.lastProgressPersistAt = Date.now();
        saveJobState(state);
      }
      resolve();
    });
  });
}

function clampNumber(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function normalizeScenes(scenes) {
  const list = Array.isArray(scenes) ? scenes : [];
  return list
    .map((s, idx) => ({
      id: Number(s?.id || idx + 1),
      heading: String(s?.heading || `Scene ${idx + 1}`).trim(),
      onScreenText: String(s?.onScreenText || "").trim(),
      durationSec: clampNumber(s?.durationSec, 1, 1200, 6),
      imageUrl: toLocalMediaUrl(String(s?.imageUrl || "").trim(), "image"),
      audioUrl: toLocalMediaUrl(String(s?.audioUrl || "").trim(), "audio"),
    }))
    .filter((s) => s.durationSec > 0 && (s.imageUrl || s.onScreenText));
}

function toLocalMediaUrl(url, type) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (type === "audio") {
    const audioMatch = raw.match(/\/tts\/audio\/([^/?#]+)/i) || raw.match(/\/user\/tts\/audio\/([^/?#]+)/i);
    if (audioMatch?.[1]) {
      const fileName = decodeURIComponent(audioMatch[1]);
      const localPath = path.join(GENERATED_AUDIO_DIR, path.basename(fileName));
      if (fs.existsSync(localPath)) {
        return `${RENDER_MEDIA_BASE_URL.replace(/\/+$/, "")}/tts/audio/${encodeURIComponent(path.basename(fileName))}`;
      }
    }
    return raw;
  }
  if (raw.startsWith("data:image/")) return raw;
  if (raw.startsWith("file://")) return raw;

  const assetMatch = raw.match(/\/content\/assets\/([^/?#]+)/i) || raw.match(/\/user\/content\/assets\/([^/?#]+)/i);
  if (assetMatch?.[1]) {
    const fileName = decodeURIComponent(assetMatch[1]);
    const localPath = path.join(GENERATED_ASSETS_DIR, path.basename(fileName));
    if (fs.existsSync(localPath)) {
      const dataUrl = fileToDataUrl(localPath);
      return dataUrl || pathToFileURL(localPath).href;
    }
    return raw;
  }

  return raw;
}

function fileToDataUrl(filePath) {
  try {
    const ext = String(path.extname(filePath) || "").toLowerCase();
    const mime =
      ext === ".jpg" || ext === ".jpeg"
        ? "image/jpeg"
        : ext === ".webp"
          ? "image/webp"
          : "image/png";
    const bin = fs.readFileSync(filePath);
    return `data:${mime};base64,${bin.toString("base64")}`;
  } catch (_) {
    return "";
  }
}

async function createRenderJob({ scenes, format, title, qualityMode }) {
  ensureDirs();
  const normalizedScenes = normalizeScenes(scenes);
  if (!normalizedScenes.length) throw new Error("No valid scenes provided for render");

  const fps = VIDEO_FPS;
  const totalFrames = Math.max(
    1,
    Math.round(normalizedScenes.reduce((sum, scene) => sum + scene.durationSec, 0) * fps),
  );

  const jobId = crypto.randomUUID();
  const safeTitle = safeFileName(title || "content-video");
  const fileName = `${Date.now()}-${safeTitle}-${jobId.slice(0, 8)}.mp4`;
  const outputPath = path.join(GENERATED_DIR, fileName);

  const dimensions =
    format === "landscape"
      ? { width: 1920, height: 1080 }
      : format === "square"
        ? { width: 1080, height: 1080 }
        : { width: 1080, height: 1920 };

  const props = {
    title: String(title || "AI Content Video"),
    scenes: normalizedScenes,
    fps,
    width: dimensions.width,
    height: dimensions.height,
  };
  const estimatedRenderSeconds = estimateRenderSeconds(totalFrames, qualityMode);

  RENDER_JOBS.set(jobId, {
    id: jobId,
    status: "queued",
    progress: 0,
    lastProgressPersistAt: Date.now(),
    renderPid: null,
    renderStartedAt: null,
    qualityMode: String(qualityMode || "standard").toLowerCase(),
    estimatedRenderSeconds,
    fileName,
    filePath: outputPath,
    createdAt: new Date().toISOString(),
    finishedAt: null,
    error: null,
    lastStdout: "",
    lastStderr: "",
  });
  saveJobState(RENDER_JOBS.get(jobId));

  (async () => {
    const state = RENDER_JOBS.get(jobId);
    if (!state) return;
    state.status = "rendering";
    saveJobState(state);
    try {
      await runRender(jobId, {
        props,
        durationInFrames: totalFrames,
        outputPath,
        qualityMode,
      });
      state.status = "completed";
      state.progress = 100;
      state.finishedAt = new Date().toISOString();
      saveJobState(state);
    } catch (err) {
      state.status = "failed";
      state.error = err?.message || "Render failed";
      state.finishedAt = new Date().toISOString();
      saveJobState(state);
    }
  })();

  return { jobId, fileName, estimatedRenderSeconds };
}

function getRenderJob(jobId) {
  const id = String(jobId || "");
  const mem = RENDER_JOBS.get(id);
  if (mem) {
    const next = projectProgressFromElapsed(mem);
    if (next > Number(mem.progress || 0)) {
      mem.progress = next;
      saveJobState(mem);
    }
    return reconcileRenderingState(mem);
  }
  const disk = readJobState(id);
  if (!disk) throw new Error("Render job not found");
  const next = projectProgressFromElapsed(disk);
  if (next > Number(disk.progress || 0)) {
    disk.progress = next;
    saveJobState(disk);
  }
  const reconciled = reconcileRenderingState(disk);
  RENDER_JOBS.set(id, reconciled);
  return reconciled;
}

module.exports = {
  saveSceneImage,
  getAssetPath,
  getVideoPath,
  createRenderJob,
  getRenderJob,
};
