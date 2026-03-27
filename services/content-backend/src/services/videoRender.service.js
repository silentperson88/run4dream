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
const REMOTION_TEXT_COMPOSITION_ID = "NewsScriptHighlights";
const REMOTION_NEWS_APPROACH_ONE_ID = "NewsApproachOneVideo";
const REMOTION_NEWS_SEQUENCE_ID = "NewsApproachSequenceVideo";
const REMOTION_NEWS_CONTENT_LANDSCAPE_ID = "NewsContentVideoLandscape";
const REMOTION_NEWS_CONTENT_SHORT_ID = "NewsContentVideoShort";
const VIDEO_FPS = 30;
const PROGRESS_SAVE_STEP = 2; // persist at least each +2%
const PROGRESS_SAVE_MIN_MS = 1500; // and not more often than this interval
const QUALITY_PRESETS = {
  draft: { crf: 30, preset: "veryfast", speedFactor: 1.2 },
  standard: { crf: 23, preset: "medium", speedFactor: 2.2 },
  high: { crf: 18, preset: "slow", speedFactor: 3.8 },
  gpu: { crf: 23, preset: "p4", speedFactor: 0.9 },
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
    const isNewsContent = String(job.renderMode || "").toLowerCase() === "news_content";
    const finalizationStallLimit = isNewsContent ? estimated + 3600 : estimated + 180;
    if (Number(job.progress || 0) >= 99 && elapsedSec > finalizationStallLimit) {
      job.status = "failed";
      job.error =
        job.error ||
        `Render stalled at finalization (99%) for too long (${Math.round(elapsedSec)}s)`;
      job.finishedAt = new Date().toISOString();
      saveJobState(job);
      return job;
    }
    const maxAllowed = isNewsContent
      ? Math.max(estimated * 8, estimated + 3600)
      : Math.max(estimated * 4, estimated + 1200); // 4x estimate or +20 min
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

  const isFfmpeg = String(job.renderMode || "").toLowerCase().includes("ffmpeg");

  if (isFfmpeg && !job.renderPid) {
    job.status = "failed";
    job.error =
      job.error ||
      job.lastStderr ||
      job.lastStdout ||
      "FFmpeg failed to start or exited before producing output";
    job.finishedAt = new Date().toISOString();
    saveJobState(job);
    return job;
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

function resolveCompositionId(renderMode) {
  const mode = String(renderMode || "").toLowerCase();
  if (mode === "news_sequence" || mode === "news_approach_sequence") {
    return REMOTION_NEWS_SEQUENCE_ID;
  }
  if (mode === "news_approach_1" || mode === "new_approach_1") {
    return REMOTION_NEWS_APPROACH_ONE_ID;
  }
  if (mode === "text_news" || mode === "text-only" || mode === "text") {
    return REMOTION_TEXT_COMPOSITION_ID;
  }
  if (mode === "news_content_landscape") {
    return REMOTION_NEWS_CONTENT_LANDSCAPE_ID;
  }
  if (mode === "news_content_short") {
    return REMOTION_NEWS_CONTENT_SHORT_ID;
  }
  return REMOTION_COMPOSITION_ID;
}

function runRender(jobId, { props, durationInFrames, renderFrameEnd, outputPath, qualityMode, compositionId, useGpu }) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(FRONTEND_ROOT)) {
      reject(new Error(`Remotion project root not found: ${FRONTEND_ROOT}`));
      return;
    }

    const propsFilePath = path.join(GENERATED_DIR, `render-props-${jobId}.json`);
    fs.writeFileSync(propsFilePath, JSON.stringify(props), "utf8");

    const preset = getQualityPreset(qualityMode);
    const cli = getRemotionCliCmd();
    const totalFrames = Math.max(1, Number(durationInFrames || 1));
    const frameEnd = Number.isFinite(Number(renderFrameEnd))
      ? Math.max(0, Math.min(totalFrames - 1, Math.floor(Number(renderFrameEnd))))
      : totalFrames - 1;
    const renderedFrames = frameEnd + 1;
    const args = [
      ...cli.argsPrefix,
      "render",
      REMOTION_ENTRY,
      compositionId,
      outputPath,
      `--props=${propsFilePath}`,
      "--frames",
      `0-${frameEnd}`,
      "--codec",
      "h264",
      "--overwrite",
    ];
    if (useGpu) {
      args.push(
        "--ffmpeg-override",
        "-c:v h264_nvenc -preset p4 -rc vbr -cq 23 -b:v 0 -pix_fmt yuv420p"
      );
    } else {
      args.push("--crf", String(preset.crf), "--x264-preset", String(preset.preset));
    }

    const child = spawn(cli.cmd, args, {
      cwd: FRONTEND_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    });
    const stateAtStart = RENDER_JOBS.get(jobId);
    if (stateAtStart) {
      stateAtStart.renderPid = Number(child.pid || 0) || null;
      stateAtStart.renderStartedAt = new Date().toISOString();
      stateAtStart.lastStdout = stateAtStart.lastStdout || args.join(" ").slice(0, 4000);
      saveJobState(stateAtStart);
    }
    const estimatedSecs = Number(stateAtStart?.estimatedRenderSeconds || 0);
    const startedAtMs = Date.now();
    const baseTimeoutMs = Math.max(
      10 * 60 * 1000,
      Math.round((estimatedSecs > 0 ? estimatedSecs : 300) * 3 * 1000),
    );
    const isNewsContent = String(stateAtStart?.renderMode || "") === "news_content";
    const hardTimeoutMs = isNewsContent ? Math.max(baseTimeoutMs, 60 * 60 * 1000) : baseTimeoutMs;
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
      const pct = extractProgressPercent(msg, renderedFrames);
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
      const pct = extractProgressPercent(msg, renderedFrames);
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

function runFfmpegRender(jobId, { segments, audioPath, outputPath, width, height, fps, qualityMode, overlayText = true, showHeader = true }) {
  return new Promise((resolve, reject) => {
    const ffmpegCmd = "ffmpeg";
    const safeWidth = Math.max(320, Number(width || 1280));
    const safeHeight = Math.max(320, Number(height || 720));
    const safeFps = Math.max(12, Number(fps || 24));
    const preset = getQualityPreset(qualityMode);
    const fontFileRaw =
      process.env.FFMPEG_FONT_FILE ||
      "C:/Windows/Fonts/arial.ttf";

    const fontCacheDir = path.join(GENERATED_DIR, "fonts");
    if (!fs.existsSync(fontCacheDir)) {
      fs.mkdirSync(fontCacheDir, { recursive: true });
    }

    const normalizePath = (inputPath) => String(inputPath || "").replace(/\\/g, "/");
    const escapeFilterPath = (inputPath) =>
      normalizePath(inputPath)
        .replace(/:/g, "\\:")
        .replace(/,/g, "\\,");

    let resolvedFont = normalizePath(fontFileRaw);
    if (resolvedFont && fs.existsSync(resolvedFont)) {
      const baseName = path.basename(resolvedFont);
      if (baseName.includes(",")) {
        const safeName = baseName.replace(/,/g, "_");
        const safePath = path.join(fontCacheDir, safeName);
        if (!fs.existsSync(safePath)) {
          fs.copyFileSync(resolvedFont, safePath);
        }
        resolvedFont = normalizePath(safePath);
      }
    }

    const fontFile = escapeFilterPath(resolvedFont || "C:/Windows/Fonts/arial.ttf");

    const blackPng = path.join(GENERATED_DIR, "ffmpeg-black.png");
    if (!fs.existsSync(blackPng)) {
      const blackBase64 =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAAWgmWQ0AAAAASUVORK5CYII=";
      fs.writeFileSync(blackPng, Buffer.from(blackBase64, "base64"));
    }
    const inputFiles = [];
    segments.forEach((segment) => {
      const duration = Math.max(0.1, Number(segment.durationSec || 0.1));
      const filePath = segment.path ? String(segment.path).replace(/\\/g, "/") : blackPng.replace(/\\/g, "/");
      inputFiles.push({ path: filePath, duration });
    });

    const perInputFilters = [];
    const concatLabels = [];
    inputFiles.forEach((input, idx) => {
      perInputFilters.push(
        `[${idx}:v]scale=${safeWidth}:${safeHeight}:force_original_aspect_ratio=increase,` +
          `crop=${safeWidth}:${safeHeight},fps=${safeFps},setsar=1,format=yuv420p,setpts=PTS-STARTPTS[v${idx}]`,
      );
      concatLabels.push(`[v${idx}]`);
    });

    let filterChain = `${perInputFilters.join(";")};${concatLabels.join("")}concat=n=${inputFiles.length}:v=1:a=0[base]`;
    filterChain += `;[base]setpts=PTS-STARTPTS[base0]`;

    const postFilters = [];
    if (showHeader) {
      postFilters.push(`drawbox=x=36:y=22:w=520:h=110:color=black@0.65:t=fill`);
      postFilters.push(
        `drawtext=fontfile='${fontFile}':text='R4D NEWS':x=52:y=34:fontsize=48:fontcolor=white:shadowcolor=black:shadowx=2:shadowy=2`,
      );
      postFilters.push(
        `drawtext=fontfile='${fontFile}':text='Subscribe for daily updates':x=52:y=86:fontsize=22:fontcolor=#93c5fd`,
      );
    }

    const overlayTextDir = path.join(GENERATED_DIR, `ffmpeg-text-${jobId}`);
    if (!fs.existsSync(overlayTextDir)) {
      fs.mkdirSync(overlayTextDir, { recursive: true });
    }

    (overlays || []).forEach((overlay, overlayIdx) => {
      const start = Math.max(0, Number(overlay.startSec || 0));
      const end = Math.max(start + 0.01, Number(overlay.endSec || start + 0.01));
      const text = String(overlay.text || "");
      if (text) {
        postFilters.push(
          `drawbox=x=40:y=${Math.round(safeHeight * 0.68)}:w=${safeWidth - 80}:h=${Math.round(safeHeight * 0.22)}:color=black@0.6:t=fill:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
        const textFile = path.join(overlayTextDir, `overlay-${String(overlayIdx).padStart(3, "0")}.txt`);
        fs.writeFileSync(textFile, text, "utf8");
        const textFileEscaped = escapeFilterPath(textFile);
        postFilters.push(
          `drawtext=fontfile='${fontFile}':textfile='${textFileEscaped}':x=60:y=${Math.round(safeHeight * 0.72)}:fontsize=36:fontcolor=white:line_spacing=6:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
      }
      if (overlay.showFooter) {
        postFilters.push(
          `drawbox=x=40:y=${Math.round(safeHeight * 0.9)}:w=${safeWidth - 80}:h=${Math.round(safeHeight * 0.08)}:color=black@0.6:t=fill:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
        postFilters.push(
          `drawtext=fontfile='${fontFile}':text='R4D News':x=60:y=${Math.round(safeHeight * 0.91)}:fontsize=22:fontcolor=white:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
        postFilters.push(
          `drawtext=fontfile='${fontFile}':text='Thanks for watching - Like, Share & Subscribe':x=60:y=${Math.round(safeHeight * 0.94)}:fontsize=18:fontcolor=#bae6fd:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
      }
    });

    const postChain = postFilters.length ? postFilters.join(",") : "null";
    filterChain += `;[base0]${postFilters.length ? "," : ""}${postChain}[v]`;

    const filterScript = path.join(GENERATED_DIR, `ffmpeg-filter-${jobId}.txt`);
    fs.writeFileSync(filterScript, filterChain, "utf8");

    const inputs = [];
    inputFiles.forEach((input) => {
      inputs.push(
        "-loop",
        "1",
        "-framerate",
        String(safeFps),
        "-t",
        input.duration.toFixed(3),
        "-i",
        input.path,
      );
    });
    const audioIndex = audioPath ? inputFiles.length : -1;
    if (audioPath) inputs.push("-i", audioPath);

    const args = [
      "-y",
      ...inputs,
      "-fps_mode",
      "cfr",
      "-filter_complex_script",
      filterScript,
      "-map",
      "[v]",
    ];
    if (audioIndex >= 0) {
      args.push("-map", `${audioIndex}:a`, "-shortest");
    }

    args.push(
      "-avoid_negative_ts",
      "make_zero",
      "-force_key_frames",
      "0",
      "-g",
      String(safeFps),
      "-keyint_min",
      String(safeFps),
      "-sc_threshold",
      "0",
      "-r",
      String(safeFps),
      "-c:v",
      "libx264",
      "-preset",
      String(preset.preset),
      "-crf",
      String(preset.crf),
      "-pix_fmt",
      "yuv420p",
      outputPath
    );

  const child = spawn(ffmpegCmd, args, {
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    });

    const stateAtStart = RENDER_JOBS.get(jobId);
    if (stateAtStart) {
      stateAtStart.renderPid = Number(child.pid || 0) || null;
      stateAtStart.renderStartedAt = new Date().toISOString();
      saveJobState(stateAtStart);
    }

    let stderr = "";
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (err) => {
      const state = RENDER_JOBS.get(jobId);
      if (state) {
        state.status = "failed";
        state.error = err?.message || "FFmpeg render failed";
        state.finishedAt = new Date().toISOString();
        state.renderPid = null;
        state.lastStdout = stdout.slice(-4000);
        state.lastStderr = stderr.slice(-4000);
        state.lastProgressPersistAt = Date.now();
        saveJobState(state);
      }
      reject(err);
    });
    child.on("close", (code) => {
      if (code !== 0) {
        const error = stderr || stdout || `FFmpeg failed with code ${code}`;
        const state = RENDER_JOBS.get(jobId);
        if (state) {
          state.status = "failed";
          state.error = error;
          state.finishedAt = new Date().toISOString();
          state.renderPid = null;
          state.lastStdout = stdout.slice(-4000);
          state.lastStderr = stderr.slice(-4000);
          state.lastProgressPersistAt = Date.now();
          saveJobState(state);
        }
        reject(new Error(error));
        return;
      }
      let outputOk = false;
      try {
        const stat = fs.statSync(outputPath);
        outputOk = stat.size > 0;
      } catch (_) {
        outputOk = false;
      }

      const state = RENDER_JOBS.get(jobId);
      if (!outputOk) {
        const error = "FFmpeg completed but output file was not created";
        if (state) {
          state.status = "failed";
          state.error = error;
          state.finishedAt = new Date().toISOString();
          state.renderPid = null;
          state.lastStdout = stdout.slice(-4000);
          state.lastStderr = stderr.slice(-4000);
          state.lastProgressPersistAt = Date.now();
          saveJobState(state);
        }
        reject(new Error(error));
        return;
      }

      if (state) {
        state.status = "completed";
        state.progress = 100;
        state.finishedAt = new Date().toISOString();
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



function runFfmpegTimelineRender(jobId, { segments, overlays, mediaOverlays, audioPath, outputPath, width, height, fps, qualityMode }) {
  return new Promise((resolve, reject) => {
    const ffmpegCmd = "ffmpeg";
    const safeWidth = Math.max(320, Number(width || 1280));
    const safeHeight = Math.max(320, Number(height || 720));
    const safeFps = Math.max(12, Number(fps || 24));
    const preset = getQualityPreset(qualityMode);

    const fontFileRaw = process.env.FFMPEG_FONT_FILE || "C:/Windows/Fonts/arial.ttf";
    const normalizePath = (inputPath) => String(inputPath || "").replace(/\\/g, "/");
    const escapeFilterPath = (inputPath) =>
      normalizePath(inputPath)
        .replace(/:/g, "\\:")
        .replace(/,/g, "\\,");

    const fontFile = escapeFilterPath(fontFileRaw || "C:/Windows/Fonts/arial.ttf");

    const blackPng = path.join(GENERATED_DIR, "ffmpeg-black.png");
    if (!fs.existsSync(blackPng)) {
      const blackBase64 =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAAWgmWQ0AAAAASUVORK5CYII=";
      fs.writeFileSync(blackPng, Buffer.from(blackBase64, "base64"));
    }

    const minSegmentSec = 1 / safeFps;
    const inputFiles = (Array.isArray(segments) ? segments : []).map((segment) => {
      const duration = Math.max(minSegmentSec, Number(segment?.durationSec || minSegmentSec));
      const filePath = segment?.path ? String(segment.path).replace(/\\\\/g, "/") : blackPng.replace(/\\\\/g, "/");
      const transition = String(segment?.transition || 'fade');
      return { path: filePath, duration, transition };
    });

    const perInputFilters = [];
    const concatLabels = [];
    const buildSegmentFilter = (input, idx) => {
      return `[` + idx + `:v]scale=${safeWidth}:${safeHeight}:force_original_aspect_ratio=increase,crop=${safeWidth}:${safeHeight},fps=${safeFps},format=yuv420p,setsar=1,setpts=PTS-STARTPTS[v${idx}]`;
    };

    inputFiles.forEach((input, idx) => {
      perInputFilters.push(buildSegmentFilter(input, idx));
      concatLabels.push(`[v${idx}]`);
    });

    let filterChain = `${perInputFilters.join(";")};${concatLabels.join("")}concat=n=${inputFiles.length}:v=1:a=0[base]`;
    filterChain += `;[base]setpts=PTS-STARTPTS[base0]`;

    const overlayTextDir = path.join(GENERATED_DIR, `ffmpeg-text-${jobId}`);
    if (!fs.existsSync(overlayTextDir)) {
      fs.mkdirSync(overlayTextDir, { recursive: true });
    }

    const overlayFilters = [];
    const boxX = 60;
    const boxY = Math.round(safeHeight * 0.72);
    const boxH = Math.round(safeHeight * 0.16);
    const boxW = safeWidth - (boxX * 2);
    
    (Array.isArray(overlays) ? overlays : []).forEach((overlay, overlayIdx) => {
      const start = Math.max(0, Number(overlay?.startSec || 0));
      const end = Math.max(start + 0.01, Number(overlay?.endSec || start + 0.01));
      const text = String(overlay?.text || "").trim();
      if (text) {
        overlayFilters.push(
          `drawbox=x=${boxX}:y=${boxY}:w=${boxW}:h=${boxH}:color=black@0.6:t=fill:enable='between(t\\,${start.toFixed(3)}\\,${end.toFixed(3)})'`,
        );

        const words = text.split(/\s+/).filter(Boolean);
        if (words.length <= 1) {
          const textFile = path.join(overlayTextDir, `overlay-${String(overlayIdx).padStart(3, "0")}.txt`);
          fs.writeFileSync(textFile, text, "utf8");
          const textFileEscaped = escapeFilterPath(textFile);
          overlayFilters.push(
            `drawtext=fontfile='${fontFile}':textfile='${textFileEscaped}':x=(w-text_w)/2:y=(${boxY}+( ${boxH}-text_h)/2):fontsize=36:fontcolor=white:line_spacing=6:enable='between(t\\,${start.toFixed(3)}\\,${end.toFixed(3)})'`,
          );
        } else {
          const total = Math.max(0.01, end - start);
          const step = total / words.length;
          let cumulative = "";
          words.forEach((word, wordIdx) => {
            cumulative = cumulative ? `${cumulative} ${word}` : word;
            const wStart = start + step * wordIdx;
            const wEnd = wordIdx === words.length - 1 ? end : start + step * (wordIdx + 1);
            const textFile = path.join(
              overlayTextDir,
              `overlay-${String(overlayIdx).padStart(3, "0")}-${String(wordIdx).padStart(3, "0")}.txt`,
            );
            fs.writeFileSync(textFile, cumulative, "utf8");
            const textFileEscaped = escapeFilterPath(textFile);
            overlayFilters.push(
              `drawtext=fontfile='${fontFile}':textfile='${textFileEscaped}':x=(w-text_w)/2:y=(${boxY}+( ${boxH}-text_h)/2):fontsize=36:fontcolor=white:line_spacing=6:enable='between(t\\,${wStart.toFixed(3)}\\,${wEnd.toFixed(3)})'`,
            );
          });
        }
      }
      if (overlay?.showFooter) {
        overlayFilters.push(
          `drawbox=x=40:y=${Math.round(safeHeight * 0.9)}:w=${safeWidth - 80}:h=${Math.round(safeHeight * 0.08)}:color=black@0.6:t=fill:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
        overlayFilters.push(
          `drawtext=fontfile='${fontFile}':text='R4D News':x=60:y=${Math.round(safeHeight * 0.91)}:fontsize=22:fontcolor=white:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
        overlayFilters.push(
          `drawtext=fontfile='${fontFile}':text='Thanks for watching - Like, Share & Subscribe':x=60:y=${Math.round(safeHeight * 0.94)}:fontsize=18:fontcolor=#bae6fd:enable='between(t\,${start.toFixed(3)}\,${end.toFixed(3)})'`,
        );
      }
    });

    let currentLabel = "base0";
    const postChain = overlayFilters.length ? overlayFilters.join(",") : "";
    if (postChain) {
      filterChain += `;[${currentLabel}]${postChain}[vtext]`;
      currentLabel = "vtext";
    }

    const totalDurationSec = inputFiles.reduce((sum, input) => sum + Number(input.duration || 0), 0);
    const overlayItems = (Array.isArray(mediaOverlays) ? mediaOverlays : []).flatMap((item) => {
      const start = Math.max(0, Number(item?.startSec || 0));
      const end = Math.max(start + 0.01, Number(item?.endSec || start + 0.01));
      const duration = Math.max(0.05, end - start);
      const scheduleMode =
        String(item?.scheduleMode || "").toLowerCase() === "timeline_list"
          ? "timeline_list"
          : String(item?.scheduleMode || "").toLowerCase() === "random"
            ? "random"
            : String(item?.scheduleMode || "").toLowerCase() === "interval" || item?.repeat
              ? "interval"
              : "single";

      if (scheduleMode === "timeline_list") {
        const starts = String(item?.timelineStarts || "")
          .split(",")
          .map((entry) => Number(String(entry || "").trim()))
          .filter((value) => Number.isFinite(value) && value >= 0);
        return starts.map((nextStart) => ({
          ...item,
          startSec: nextStart,
          endSec: Math.min(totalDurationSec, nextStart + duration),
        }));
      }

      if (scheduleMode === "random") {
        const count = Math.max(1, Number(item?.randomCount || 1));
        const randomDurationSec = Math.max(0.5, Number(item?.randomDurationSec || duration));
        const windowStart = start;
        const windowEnd = Math.max(windowStart + randomDurationSec, end);
        const available = Math.max(0, windowEnd - windowStart - randomDurationSec);
        const seedSource = `${String(item?.url || "")}:${String(item?.label || "")}:${String(item?.position || "")}:${String(item?.size || "")}`;
        let seed = 0;
        for (let i = 0; i < seedSource.length; i += 1) {
          seed = (seed * 31 + seedSource.charCodeAt(i)) % 2147483647;
        }
        const randomStarts = [];
        for (let i = 0; i < count; i += 1) {
          seed = (seed * 48271) % 2147483647;
          const ratio = seed / 2147483647;
          const nextStart = Number((windowStart + ratio * available).toFixed(3));
          randomStarts.push(nextStart);
        }
        randomStarts.sort((a, b) => a - b);
        return randomStarts.map((nextStart) => ({
          ...item,
          startSec: nextStart,
          endSec: Math.min(totalDurationSec, nextStart + randomDurationSec),
        }));
      }

      if (scheduleMode !== "interval") {
        return [{ ...item, startSec: start, endSec: end }];
      }

      const repeatEverySec = Math.max(duration, Number(item?.repeatEverySec || 30));
      const expanded = [];
      for (let nextStart = start; nextStart < totalDurationSec; nextStart += repeatEverySec) {
        expanded.push({
          ...item,
          startSec: nextStart,
          endSec: Math.min(totalDurationSec, nextStart + duration),
        });
      }
      return expanded;
    });
    const overlayMargin = 36;
    const overlaySizeMap = {
      sm: Math.round(safeWidth * 0.12),
      md: Math.round(safeWidth * 0.18),
      lg: Math.round(safeWidth * 0.24),
    };
    const overlayInputs = [];
    const overlayFiltersChain = [];
    overlayItems.forEach((item, idx) => {
      const start = Math.max(0, Number(item?.startSec || 0));
      const end = Math.max(start + 0.01, Number(item?.endSec || start + 0.01));
      const duration = Math.max(0.05, end - start);
      const type = String(item?.type || "image").toLowerCase() === "video" ? "video" : "image";
      const overlayPath = item?.url ? String(item.url).replace(/\\\\/g, "/") : "";
      if (!overlayPath) return;
      const sizeKey = String(item?.size || "md");
      const targetSize = overlaySizeMap[sizeKey] || overlaySizeMap.md;
      const pos = String(item?.position || "bottom-right");
      const animation = String(item?.animation || "none").toLowerCase();
      const inputIndex = inputFiles.length + idx;
      overlayInputs.push({ type, path: overlayPath, duration });

      const x =
        pos === "top-left"
          ? overlayMargin
          : pos === "top-right"
            ? safeWidth - targetSize - overlayMargin
            : pos === "bottom-left"
              ? overlayMargin
              : pos === "center"
                ? Math.round((safeWidth - targetSize) / 2)
                : safeWidth - targetSize - overlayMargin;
      const y =
        pos === "top-left"
          ? overlayMargin
          : pos === "top-right"
            ? overlayMargin
            : pos === "bottom-left"
              ? safeHeight - targetSize - overlayMargin
              : pos === "center"
                ? Math.round((safeHeight - targetSize) / 2)
                : safeHeight - targetSize - overlayMargin;

      let overlayFilter = `[${inputIndex}:v]scale=${targetSize}:${targetSize}:force_original_aspect_ratio=decrease,format=rgba`;
      if (animation === "fade") {
        const fadeSeconds = Math.min(0.4, Math.max(0.1, duration / 4));
        const fadeOutStart = Math.max(0, duration - fadeSeconds);
        overlayFilter += `,fade=t=in:st=0:d=${fadeSeconds.toFixed(3)},fade=t=out:st=${fadeOutStart.toFixed(3)}:d=${fadeSeconds.toFixed(3)}`;
      }
      overlayFilter += `[ovr${idx}]`;
      overlayFiltersChain.push(overlayFilter);
      overlayFiltersChain.push(
        `[${currentLabel}][ovr${idx}]overlay=x=${x}:y=${y}:enable='between(t\\,${start.toFixed(3)}\\,${end.toFixed(3)})'[${currentLabel}o${idx}]`,
      );
      currentLabel = `${currentLabel}o${idx}`;
    });

    if (overlayFiltersChain.length) {
      filterChain += `;${overlayFiltersChain.join(";")}`;
    }
    filterChain += `;[${currentLabel}]copy[v]`;

    const filterScript = path.join(GENERATED_DIR, `ffmpeg-filter-${jobId}.txt`);
    fs.writeFileSync(filterScript, filterChain, "utf8");

    const inputs = [];
    inputFiles.forEach((input) => {
      inputs.push("-loop", "1", "-framerate", String(safeFps), "-t", input.duration.toFixed(3), "-i", input.path);
    });
    overlayInputs.forEach((overlay) => {
      if (overlay.type === "video") {
        inputs.push("-stream_loop", "-1", "-i", overlay.path);
      } else {
        inputs.push("-loop", "1", "-t", overlay.duration.toFixed(3), "-i", overlay.path);
      }
    });
    const audioIndex = audioPath ? inputFiles.length + overlayInputs.length : -1;
    if (audioPath) inputs.push("-i", audioPath);

    const args = [
      "-y",
      ...inputs,
      "-fps_mode",
      "cfr",
      "-filter_complex_script",
      filterScript,
      "-map",
      "[v]",
    ];
    if (audioIndex >= 0) {
      args.push("-map", `${audioIndex}:a`, "-shortest");
    }

    args.push(
      "-r",
      String(safeFps),
      "-c:v",
      "libx264",
      "-preset",
      String(preset.preset),
      "-crf",
      String(preset.crf),
      "-pix_fmt",
      "yuv420p",
      outputPath,
    );

    const child = spawn(ffmpegCmd, args, { stdio: ["ignore", "pipe", "pipe"], shell: false });

    const stateAtStart = RENDER_JOBS.get(jobId);
    if (stateAtStart) {
      stateAtStart.renderPid = Number(child.pid || 0) || null;
      stateAtStart.renderStartedAt = new Date().toISOString();
      saveJobState(stateAtStart);
    }

    let stderr = "";
    let stdout = "";
    child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

    child.on("error", (err) => {
      const state = RENDER_JOBS.get(jobId);
      if (state) {
        state.status = "failed";
        state.error = err?.message || "FFmpeg render failed";
        state.finishedAt = new Date().toISOString();
        state.renderPid = null;
        state.lastStdout = stdout.slice(-4000);
        state.lastStderr = stderr.slice(-4000);
        state.lastProgressPersistAt = Date.now();
        saveJobState(state);
      }
      reject(err);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        const error = stderr || stdout || `FFmpeg failed with code ${code}`;
        const state = RENDER_JOBS.get(jobId);
        if (state) {
          state.status = "failed";
          state.error = error;
          state.finishedAt = new Date().toISOString();
          state.renderPid = null;
          state.lastStdout = stdout.slice(-4000);
          state.lastStderr = stderr.slice(-4000);
          state.lastProgressPersistAt = Date.now();
          saveJobState(state);
        }
        reject(new Error(error));
        return;
      }
      const state = RENDER_JOBS.get(jobId);
      if (state) {
        state.status = "completed";
        state.progress = 100;
        state.finishedAt = new Date().toISOString();
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

function normalizeScenes(scenes, renderMode) {
  const list = Array.isArray(scenes) ? scenes : [];
  const mode = String(renderMode || "").toLowerCase();
  const isTextMode = mode === "text_news" || mode === "text-only" || mode === "text";
  return list
    .map((s, idx) => ({
      id: Number(s?.id || idx + 1),
      heading: String(s?.heading || `Scene ${idx + 1}`).trim(),
      onScreenText: String(s?.onScreenText || s?.narration || s?.heading || "").trim(),
      category: String(s?.category || "").trim(),
      durationSec: clampNumber(s?.durationSec, 1, 1200, 6),
      imageUrl: toLocalMediaUrl(String(s?.imageUrl || "").trim(), "image"),
      audioUrl: toLocalMediaUrl(String(s?.audioUrl || "").trim(), "audio"),
    }))
    .filter((s) => s.durationSec > 0 && (isTextMode ? Boolean(s.onScreenText) : Boolean(s.imageUrl || s.onScreenText)));
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

async function createRenderJob({ scenes, format, title, qualityMode, renderMode, stylePreset }) {
  ensureDirs();
  const compositionId = resolveCompositionId(renderMode);
  const normalizedScenes = normalizeScenes(scenes, renderMode);
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
    stylePreset: String(stylePreset || "flash").toLowerCase(),
    fps,
    width: dimensions.width,
    height: dimensions.height,
  };

  if (compositionId === REMOTION_NEWS_SEQUENCE_ID) {
    const grouped = new Map();
    normalizedScenes.forEach((scene) => {
      const company = String(scene.heading || "").trim();
      const category = String(scene.category || "Other").trim() || "Other";
      if (!company) return;
      if (company.toLowerCase() === "market opening" || company.toLowerCase() === "closing bell") return;
      if (!grouped.has(category)) grouped.set(category, new Set());
      grouped.get(category).add(company);
    });

    props.dateLabel = String(new Date().toISOString().slice(0, 10));
    props.items = normalizedScenes.map((scene) => ({
      heading: scene.heading,
      script: scene.onScreenText,
      audioUrl: scene.audioUrl,
      category: scene.category || undefined,
    }));
    props.sceneDurationsSec = normalizedScenes.map((scene) => Number(scene.durationSec || 6));
    props.companyGroups = Array.from(grouped.entries()).map(([category, companies]) => ({
      category,
      companies: Array.from(companies),
    }));
    delete props.scenes;
    delete props.stylePreset;
    delete props.fps;
    delete props.width;
    delete props.height;
  }
  const estimatedRenderSeconds = estimateRenderSeconds(totalFrames, qualityMode);

  RENDER_JOBS.set(jobId, {
    id: jobId,
    status: "queued",
    progress: 0,
    lastProgressPersistAt: Date.now(),
    renderPid: null,
    renderStartedAt: null,
    qualityMode: String(qualityMode || "standard").toLowerCase(),
    renderMode: String(renderMode || "scene").toLowerCase(),
    compositionId,
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
        compositionId,
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

async function createNewsContentRenderJob({ props, durationInFrames, renderFrameEnd, compositionId, qualityMode, useGpu }) {
  ensureDirs();
  const fps = VIDEO_FPS;
  const totalFrames = Math.max(1, Number(durationInFrames || 1));
  const safeFrameEnd = Number.isFinite(Number(renderFrameEnd))
    ? Math.max(0, Math.min(totalFrames - 1, Math.floor(Number(renderFrameEnd))))
    : totalFrames - 1;
  const renderedFrames = safeFrameEnd + 1;
  const jobId = crypto.randomUUID();
  const fileName = `${Date.now()}-news-content-${jobId.slice(0, 8)}.mp4`;
  const outputPath = path.join(GENERATED_DIR, fileName);

  const estimatedRenderSeconds = estimateRenderSeconds(renderedFrames, qualityMode);

  RENDER_JOBS.set(jobId, {
    id: jobId,
    status: "queued",
    progress: 0,
    lastProgressPersistAt: Date.now(),
    renderPid: null,
    renderStartedAt: null,
    qualityMode: String(qualityMode || "standard").toLowerCase(),
    renderMode: String("news_content").toLowerCase(),
    compositionId,
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
        renderFrameEnd: safeFrameEnd,
        outputPath,
        qualityMode,
        compositionId,
        useGpu: Boolean(useGpu),
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




async function createNewsContentFfmpegTimelineJob({
  segments,
  overlays,
  mediaOverlays,
  audioPath,
  width,
  height,
  fps,
  qualityMode,
  meta,
}) {
  ensureDirs();
  const totalFrames = Math.max(1, Math.round((Number(fps || VIDEO_FPS) * segments.reduce((sum, s) => sum + Number(s.durationSec || 0), 0)) || 1));
  const jobId = crypto.randomUUID();
  const fileName = `${Date.now()}-news-content-ffmpeg-${jobId.slice(0, 8)}.mp4`;
  const outputPath = path.join(GENERATED_DIR, fileName);
  const estimatedRenderSeconds = estimateRenderSeconds(totalFrames, qualityMode);

  RENDER_JOBS.set(jobId, {
    id: jobId,
    status: "queued",
    progress: 0,
    lastProgressPersistAt: Date.now(),
    renderPid: null,
    renderStartedAt: null,
    qualityMode: String(qualityMode || "standard").toLowerCase(),
    renderMode: String("news_content_ffmpeg").toLowerCase(),
    compositionId: "ffmpeg",
    estimatedRenderSeconds,
    fileName,
    filePath: outputPath,
    meta: meta || {},
    createdAt: new Date().toISOString(),
    finishedAt: null,
    error: null,
    lastStdout: "",
    lastStderr: "",
  });
  saveJobState(RENDER_JOBS.get(jobId));

  setImmediate(() => {
    (async () => {
      const state = RENDER_JOBS.get(jobId);
      if (!state) return;
      state.status = "rendering";
      saveJobState(state);
      try {
        await runFfmpegTimelineRender(jobId, {
          segments,
          overlays,
          mediaOverlays,
          audioPath,
          outputPath,
          width,
          height,
          fps,
          qualityMode,
        });
        state.status = "completed";
        state.progress = 100;
        state.finishedAt = new Date().toISOString();
        saveJobState(state);
      } catch (err) {
        state.status = "failed";
        state.error = err?.message || "FFmpeg render failed";
        state.lastStderr = err?.message || state.lastStderr || "";
        state.finishedAt = new Date().toISOString();
        saveJobState(state);
      }
    })();
  });

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
  createNewsContentRenderJob,
  createNewsContentFfmpegTimelineJob,
  getRenderJob,
};



