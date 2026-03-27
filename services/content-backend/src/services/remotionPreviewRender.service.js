const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

const JOBS = new Map();
const GENERATED_DIR = path.join(__dirname, "..", "..", "generated-video");
const FRONTEND_ROOT = process.env.REMOTION_PROJECT_ROOT
  ? path.resolve(process.env.REMOTION_PROJECT_ROOT)
  : path.resolve(__dirname, "..", "..", "..", "..", "..", "run4dream_frontend");
const REMOTION_ENTRY = "src/remotion/index.ts";

const QUALITY_PRESETS = {
  draft: { crf: 30, preset: "veryfast", speedFactor: 1.2 },
  standard: { crf: 23, preset: "medium", speedFactor: 2.2 },
  high: { crf: 18, preset: "slow", speedFactor: 3.8 },
  gpu: { crf: 23, preset: "p4", speedFactor: 0.9 },
};

function ensureDirs() {
  fs.mkdirSync(GENERATED_DIR, { recursive: true });
}

function getQualityPreset(mode) {
  const key = String(mode || "high").toLowerCase();
  return QUALITY_PRESETS[key] || QUALITY_PRESETS.high;
}

function estimateRenderSeconds(frames, qualityMode) {
  const fps = 30;
  const seconds = Math.max(1, Number(frames || 1) / fps);
  const factor = getQualityPreset(qualityMode).speedFactor || 2.5;
  return Math.max(10, Math.ceil(seconds * factor));
}

function getRemotionCliCmd() {
  const nodeCmd = process.platform === "win32" ? "node.exe" : "node";
  const frontendCli = path.join(FRONTEND_ROOT, "node_modules", "@remotion", "cli", "remotion-cli.js");
  if (fs.existsSync(frontendCli)) {
    return { cmd: nodeCmd, argsPrefix: [frontendCli] };
  }
  throw new Error("Remotion CLI not found in frontend node_modules");
}

function extractProgressPercent(outputLine, expectedFrames) {
  const line = String(outputLine || "");
  const frameMatch = line.match(/(?:frame|encoded frame)\s*[:=]\s*(\d+)/i);
  if (frameMatch?.[1]) {
    const frameNum = Number(frameMatch[1]);
    if (Number.isFinite(frameNum) && frameNum >= 0) {
      const denom = Math.max(1, Number(expectedFrames || 1));
      return Math.max(1, Math.min(99, Math.round((frameNum / denom) * 100)));
    }
  }
  const pctMatch = line.match(/(\d+(?:\.\d+)?)\s*%/);
  if (pctMatch?.[1]) {
    const val = Number(pctMatch[1]);
    if (Number.isFinite(val)) return Math.max(1, Math.min(99, Math.round(val)));
  }
  return null;
}

function runRemotionRender(jobId, { props, compositionId, durationInFrames, renderFrameEnd, qualityMode, useGpu, outputPath }) {
  return new Promise((resolve, reject) => {
    ensureDirs();
    const cli = getRemotionCliCmd();
    const propsPath = path.join(GENERATED_DIR, `preview-remotion-props-${jobId}.json`);
    fs.writeFileSync(propsPath, JSON.stringify(props), "utf8");

    const totalFrames = Math.max(1, Number(durationInFrames || 1));
    const frameEnd = Number.isFinite(Number(renderFrameEnd))
      ? Math.max(0, Math.min(totalFrames - 1, Math.floor(Number(renderFrameEnd))))
      : totalFrames - 1;
    const renderedFrames = frameEnd + 1;

    const preset = getQualityPreset(qualityMode);
    const args = [
      ...cli.argsPrefix,
      "render",
      REMOTION_ENTRY,
      compositionId,
      outputPath,
      `--props=${propsPath}`,
      "--frames",
      `0-${frameEnd}`,
      "--codec",
      "h264",
      "--overwrite",
    ];
    if (useGpu) {
      args.push(
        "--ffmpeg-override",
        "-c:v h264_nvenc -preset p4 -rc vbr -cq 23 -b:v 0 -pix_fmt yuv420p",
      );
    } else {
      args.push("--crf", String(preset.crf), "--x264-preset", String(preset.preset));
    }

    const child = spawn(cli.cmd, args, {
      cwd: FRONTEND_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    });

    const state = JOBS.get(jobId);
    if (state) {
      state.renderPid = Number(child.pid || 0) || null;
      state.renderStartedAt = new Date().toISOString();
      state.lastStdout = args.join(" ").slice(0, 4000);
    }

    let stderr = "";
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      const msg = chunk.toString();
      stdout += msg;
      const s = JOBS.get(jobId);
      if (s) {
        const pct = extractProgressPercent(msg, renderedFrames);
        if (pct && pct > s.progress) s.progress = pct;
      }
    });
    child.stderr.on("data", (chunk) => {
      const msg = chunk.toString();
      stderr += msg;
      const s = JOBS.get(jobId);
      if (s) {
        const pct = extractProgressPercent(msg, renderedFrames);
        if (pct && pct > s.progress) s.progress = pct;
      }
    });

    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr || stdout || `Remotion render failed with code ${code}`));
        return;
      }
      resolve();
    });
  });
}

async function createRemotionPreviewJob({
  props,
  compositionId,
  durationInFrames,
  renderFrameEnd,
  qualityMode,
  useGpu,
}) {
  ensureDirs();
  const totalFrames = Math.max(1, Number(durationInFrames || 1));
  const frameEnd = Number.isFinite(Number(renderFrameEnd))
    ? Math.max(0, Math.min(totalFrames - 1, Math.floor(Number(renderFrameEnd))))
    : totalFrames - 1;
  const renderedFrames = frameEnd + 1;

  const jobId = crypto.randomUUID();
  const fileName = `${Date.now()}-news-content-remotion-preview-${jobId.slice(0, 8)}.mp4`;
  const outputPath = path.join(GENERATED_DIR, fileName);
  const estimatedRenderSeconds = estimateRenderSeconds(renderedFrames, qualityMode);

  JOBS.set(jobId, {
    id: jobId,
    status: "queued",
    progress: 0,
    renderPid: null,
    renderStartedAt: null,
    qualityMode: String(qualityMode || "high").toLowerCase(),
    renderMode: "news_content_remotion_preview",
    compositionId,
    estimatedRenderSeconds,
    fileName,
    filePath: outputPath,
    createdAt: new Date().toISOString(),
    finishedAt: null,
    error: null,
    lastStdout: "",
    lastStderr: "",
    meta: {
      totalFrames,
      renderedFrames,
      renderFrameEnd: frameEnd,
    },
  });

  setImmediate(async () => {
    const state = JOBS.get(jobId);
    if (!state) return;
    state.status = "rendering";
    try {
      await runRemotionRender(jobId, {
        props,
        compositionId,
        durationInFrames: totalFrames,
        renderFrameEnd: frameEnd,
        qualityMode,
        useGpu: Boolean(useGpu),
        outputPath,
      });
      state.status = "completed";
      state.progress = 100;
      state.finishedAt = new Date().toISOString();
      state.renderPid = null;
    } catch (err) {
      state.status = "failed";
      state.error = err?.message || "Remotion preview render failed";
      state.finishedAt = new Date().toISOString();
      state.renderPid = null;
    }
  });

  return { jobId, fileName, estimatedRenderSeconds };
}

function getRemotionPreviewJob(jobId) {
  const job = JOBS.get(String(jobId || ""));
  if (!job) throw new Error("Render job not found");
  return job;
}

module.exports = {
  createRemotionPreviewJob,
  getRemotionPreviewJob,
};

