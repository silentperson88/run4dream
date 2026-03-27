const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const service = require("./newsContentVideos.service");

const GENERATED_DIR = path.join(__dirname, "..", "..", "generated-video");
const CACHE_DIR = path.join(GENERATED_DIR, "gpu-fast-v2-cache");
const CACHE_SOURCES_DIR = path.join(CACHE_DIR, "sources");
const JOBS_STATE_FILE = path.join(CACHE_DIR, "jobs-state.json");
const FRONTEND_ROOT = process.env.REMOTION_PROJECT_ROOT
  ? path.resolve(process.env.REMOTION_PROJECT_ROOT)
  : path.resolve(__dirname, "..", "..", "..", "..", "..", "run4dream_frontend");
const REMOTION_ENTRY = "src/remotion/index.ts";

const MAX_JOBS = 400;
const QUEUE = [];
const JOBS = new Map();
let workerActive = false;

const QUALITY_PRESETS = {
  draft: { crf: 26, preset: "fast", speedFactor: 1.3 },
  standard: { crf: 23, preset: "medium", speedFactor: 2.2 },
  high: { crf: 18, preset: "slow", speedFactor: 3.8 },
  gpu: { crf: 21, preset: "p4", speedFactor: 0.95 },
};

function ensureDirs() {
  fs.mkdirSync(GENERATED_DIR, { recursive: true });
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  fs.mkdirSync(CACHE_SOURCES_DIR, { recursive: true });
}

function readJsonSafe(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_) {
    return fallback;
  }
}

function writeJsonSafe(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
  } catch (_) {}
}

function slugifyTitle(input) {
  return String(input || "news-video")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 70) || "news-video";
}

function hashText(input) {
  return crypto.createHash("sha1").update(String(input || "")).digest("hex");
}

function detectMimeFromPath(filePath, fallbackType) {
  const ext = String(filePath || "").toLowerCase();
  if (ext.endsWith(".jpg") || ext.endsWith(".jpeg")) return "image/jpeg";
  if (ext.endsWith(".png")) return "image/png";
  if (ext.endsWith(".webp")) return "image/webp";
  if (ext.endsWith(".mp3")) return "audio/mpeg";
  if (ext.endsWith(".wav")) return "audio/wav";
  if (fallbackType === "audio") return "audio/wav";
  return "image/png";
}

function toDataUrl(buffer, mime) {
  return `data:${mime};base64,${Buffer.from(buffer).toString("base64")}`;
}

function getQualityPreset(mode) {
  const key = String(mode || "gpu").toLowerCase();
  return QUALITY_PRESETS[key] || QUALITY_PRESETS.gpu;
}

function estimateRenderSeconds(frames, qualityMode) {
  const fps = 30;
  const seconds = Math.max(1, Number(frames || 1) / fps);
  const factor = getQualityPreset(qualityMode).speedFactor || 1;
  return Math.max(8, Math.ceil(seconds * factor));
}

function getRemotionCliCmd() {
  const nodeCmd = process.platform === "win32" ? "node.exe" : "node";
  const frontendCli = path.join(FRONTEND_ROOT, "node_modules", "@remotion", "cli", "remotion-cli.js");
  if (!fs.existsSync(frontendCli)) {
    throw new Error("Remotion CLI not found in frontend node_modules");
  }
  return { cmd: nodeCmd, argsPrefix: [frontendCli] };
}

function resolveImageLocalPath(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  const match = raw.match(/\/content\/news-content\/videos\/assets\/images\/([^/?#]+)/i);
  if (match?.[1]) {
    const fileName = decodeURIComponent(match[1]);
    const filePath = service.getImagePath(fileName);
    if (fs.existsSync(filePath)) return filePath;
  }
  if (fs.existsSync(raw)) return raw;
  return "";
}

function resolveAudioLocalPath(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  const match = raw.match(/\/content\/news-content\/videos\/assets\/audio\/([^/?#]+)/i);
  if (match?.[1]) {
    const fileName = decodeURIComponent(match[1]);
    const filePath = service.getAudioPath(fileName);
    if (fs.existsSync(filePath)) return filePath;
  }
  if (fs.existsSync(raw)) return raw;
  return "";
}

async function fetchArrayBuffer(url) {
  if (typeof fetch !== "function") throw new Error("fetch is not available in current Node runtime");
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Unable to fetch asset: ${url}`);
  const arrayBuffer = await res.arrayBuffer();
  return { buffer: Buffer.from(arrayBuffer), mime: res.headers.get("content-type") || "" };
}

async function resolveAndCacheSource(url, type) {
  const raw = String(url || "").trim();
  if (!raw || raw.startsWith("data:")) return raw;

  const cacheKey = hashText(`${type}:${raw}`);
  const cacheFilePath = path.join(CACHE_SOURCES_DIR, `${cacheKey}.json`);
  const cached = readJsonSafe(cacheFilePath, null);
  if (cached?.dataUrl) return String(cached.dataUrl);

  let buffer = null;
  let mime = "";
  const localPath = type === "audio" ? resolveAudioLocalPath(raw) : resolveImageLocalPath(raw);
  if (localPath) {
    buffer = fs.readFileSync(localPath);
    mime = detectMimeFromPath(localPath, type);
  } else if (/^https?:\/\//i.test(raw)) {
    const fetched = await fetchArrayBuffer(raw);
    buffer = fetched.buffer;
    mime = fetched.mime || detectMimeFromPath(raw, type);
  } else {
    return raw;
  }

  const dataUrl = toDataUrl(buffer, mime || detectMimeFromPath(raw, type));
  writeJsonSafe(cacheFilePath, {
    dataUrl,
    createdAt: new Date().toISOString(),
    source: raw,
    type,
  });
  return dataUrl;
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const size = Math.max(1, Number(concurrency || 1));
  const results = new Array(items.length);
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const current = index;
      index += 1;
      results[current] = await mapper(items[current], current);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(size, items.length); i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

function collectSources(props) {
  const list = [];
  if (props?.audioUrl) list.push({ path: String(props.audioUrl), type: "audio" });

  const clips = Array.isArray(props?.clips) ? props.clips : [];
  clips.forEach((clip) => {
    const images = Array.isArray(clip?.images) ? clip.images : [];
    images.forEach((img) => {
      if (img?.url) list.push({ path: String(img.url), type: "image" });
    });
  });

  const timelineSegments = Array.isArray(props?.timeline?.segments) ? props.timeline.segments : [];
  timelineSegments.forEach((segment) => {
    const src = String(segment?.path || segment?.url || "").trim();
    if (src) list.push({ path: src, type: "image" });
  });

  const mediaOverlays = Array.isArray(props?.mediaOverlays) ? props.mediaOverlays : [];
  mediaOverlays.forEach((item) => {
    const src = String(item?.url || "").trim();
    if (!src) return;
    list.push({ path: src, type: String(item?.type || "image").toLowerCase() === "video" ? "video" : "image" });
  });

  const seen = new Set();
  return list.filter((item) => {
    const key = `${item.type}:${item.path}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function updateJob(jobId, patch) {
  const job = JOBS.get(jobId);
  if (!job) return null;
  Object.assign(job, patch);
  persistJobs();
  return job;
}

function persistJobs() {
  const jobs = Array.from(JOBS.values())
    .map((job) => ({ ...job, props: undefined }))
    .sort(
    (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
  );
  writeJsonSafe(JOBS_STATE_FILE, { jobs: jobs.slice(0, MAX_JOBS) });
}

function loadPersistedJobs() {
  ensureDirs();
  const data = readJsonSafe(JOBS_STATE_FILE, { jobs: [] });
  const jobs = Array.isArray(data?.jobs) ? data.jobs : [];
  jobs.forEach((job) => {
    if (!job?.id) return;
    if (job.status === "preparing" || job.status === "rendering" || job.status === "encoding") {
      job.status = "failed";
      job.error = job.error || "Worker restarted before completion";
      job.finishedAt = job.finishedAt || new Date().toISOString();
    }
    JOBS.set(job.id, job);
  });
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

async function preparePropsWithCache(jobId, props) {
  const sources = collectSources(props);
  if (!sources.length) return props;

  const resolvedMap = new Map();
  let done = 0;
  const total = sources.length;

  await mapWithConcurrency(sources, 8, async (item) => {
    const resolved = item.type === "video" ? item.path : await resolveAndCacheSource(item.path, item.type);
    resolvedMap.set(`${item.type}:${item.path}`, resolved);
    done += 1;
    const pct = 5 + Math.round((done / total) * 25);
    updateJob(jobId, { progress: Math.max(5, Math.min(30, pct)) });
  });

  const nextProps = JSON.parse(JSON.stringify(props || {}));
  if (nextProps?.audioUrl) {
    const key = `audio:${String(nextProps.audioUrl)}`;
    nextProps.audioUrl = resolvedMap.get(key) || nextProps.audioUrl;
  }

  const clips = Array.isArray(nextProps?.clips) ? nextProps.clips : [];
  clips.forEach((clip) => {
    const images = Array.isArray(clip?.images) ? clip.images : [];
    images.forEach((img) => {
      const key = `image:${String(img?.url || "")}`;
      if (img?.url) img.url = resolvedMap.get(key) || img.url;
    });
  });

  const timelineSegments = Array.isArray(nextProps?.timeline?.segments) ? nextProps.timeline.segments : [];
  timelineSegments.forEach((segment) => {
    const src = String(segment?.path || segment?.url || "");
    if (!src) return;
    const key = `image:${src}`;
    const resolved = resolvedMap.get(key);
    if (resolved) segment.path = resolved;
  });

  const mediaOverlays = Array.isArray(nextProps?.mediaOverlays) ? nextProps.mediaOverlays : [];
  mediaOverlays.forEach((item) => {
    const itemType = String(item?.type || "image").toLowerCase() === "video" ? "video" : "image";
    const key = `${itemType}:${String(item?.url || "")}`;
    const resolved = resolvedMap.get(key);
    if (resolved) item.url = resolved;
  });

  return nextProps;
}

function runRemotionRender(jobId, { props, compositionId, durationInFrames, renderFrameEnd, qualityMode, outputPath }) {
  return new Promise((resolve, reject) => {
    const cli = getRemotionCliCmd();
    const propsPath = path.join(GENERATED_DIR, `gpu-fast-v2-props-${jobId}.json`);
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
      "--ffmpeg-override",
      `-c:v h264_nvenc -preset ${preset.preset} -rc vbr -cq ${preset.crf} -b:v 0 -pix_fmt yuv420p`,
    ];

    const child = spawn(cli.cmd, args, {
      cwd: FRONTEND_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    });

    updateJob(jobId, {
      renderPid: Number(child.pid || 0) || null,
      renderStartedAt: new Date().toISOString(),
      lastStdout: args.join(" ").slice(0, 4000),
    });

    let stdout = "";
    let stderr = "";
    const onProgress = (chunk) => {
      const msg = chunk.toString();
      const pct = extractProgressPercent(msg, renderedFrames);
      if (!pct) return;
      const mapped = 30 + Math.round((pct / 100) * 68);
      const job = JOBS.get(jobId);
      if (!job) return;
      if (mapped > Number(job.progress || 0)) {
        updateJob(jobId, { progress: Math.max(30, Math.min(98, mapped)) });
      }
    };

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      onProgress(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      onProgress(chunk);
    });

    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr || stdout || `Fast render failed with code ${code}`));
        return;
      }
      resolve();
    });
  });
}

async function processJob(jobId) {
  const job = JOBS.get(jobId);
  if (!job) return;
  try {
    updateJob(jobId, { status: "preparing", phase: "preparing_assets", progress: 5, startedAt: new Date().toISOString() });
    const preparedProps = await preparePropsWithCache(jobId, job.props);

    updateJob(jobId, { status: "rendering", phase: "rendering", progress: Math.max(30, Number(job.progress || 30)) });
    await runRemotionRender(jobId, {
      props: preparedProps,
      compositionId: job.compositionId,
      durationInFrames: job.durationInFrames,
      renderFrameEnd: job.renderFrameEnd,
      qualityMode: job.qualityMode,
      outputPath: job.filePath,
    });

    updateJob(jobId, {
      status: "completed",
      phase: "completed",
      progress: 100,
      finishedAt: new Date().toISOString(),
      renderPid: null,
      error: null,
    });
  } catch (err) {
    updateJob(jobId, {
      status: "failed",
      phase: "failed",
      finishedAt: new Date().toISOString(),
      renderPid: null,
      error: err?.message || "Fast render failed",
    });
  }
}

async function workerLoop() {
  if (workerActive) return;
  workerActive = true;
  while (QUEUE.length) {
    const nextJobId = QUEUE.shift();
    await processJob(nextJobId);
  }
  workerActive = false;
}

function enqueueJob(jobId) {
  QUEUE.push(jobId);
  setImmediate(workerLoop);
}

function trimOldJobs() {
  const sorted = Array.from(JOBS.values()).sort(
    (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
  );
  sorted.slice(MAX_JOBS).forEach((job) => JOBS.delete(job.id));
}

async function createFastGpuJob({
  userId,
  videoId,
  title,
  props,
  compositionId,
  durationInFrames,
  renderFrameEnd,
  qualityMode,
}) {
  ensureDirs();
  trimOldJobs();

  const jobId = crypto.randomUUID();
  const safeTitle = slugifyTitle(title);
  const fileName = `${Date.now()}-${safeTitle}-${jobId.slice(0, 8)}.mp4`;
  const filePath = path.join(GENERATED_DIR, fileName);
  const estimatedRenderSeconds = estimateRenderSeconds(durationInFrames, qualityMode || "gpu");

  JOBS.set(jobId, {
    id: jobId,
    userId: Number(userId || 0),
    videoId: Number(videoId || 0),
    title: String(title || "R4D News"),
    status: "queued",
    phase: "queued",
    progress: 0,
    renderPid: null,
    renderStartedAt: null,
    startedAt: null,
    finishedAt: null,
    compositionId: String(compositionId || "NewsContentVideoLandscape"),
    durationInFrames: Math.max(1, Number(durationInFrames || 1)),
    renderFrameEnd: Number.isFinite(Number(renderFrameEnd))
      ? Math.max(0, Math.floor(Number(renderFrameEnd)))
      : Math.max(0, Math.max(1, Number(durationInFrames || 1)) - 1),
    qualityMode: String(qualityMode || "gpu"),
    estimatedRenderSeconds,
    fileName,
    filePath,
    videoUrl: `content/news-content/videos/rendered/${fileName}`,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    error: null,
    props,
  });

  persistJobs();
  enqueueJob(jobId);
  return { jobId, fileName, estimatedRenderSeconds, videoUrl: `content/news-content/videos/rendered/${fileName}` };
}

function getFastGpuJob(jobId) {
  const job = JOBS.get(String(jobId || ""));
  if (!job) throw new Error("Render job not found");
  return { ...job, props: undefined };
}

function listFastGpuJobs({ userId, inProgressOnly = false } = {}) {
  const rows = Array.from(JOBS.values())
    .filter((job) => (Number(userId || 0) ? Number(job.userId || 0) === Number(userId || 0) : true))
    .filter((job) =>
      inProgressOnly
        ? ["queued", "preparing", "rendering", "encoding"].includes(String(job.status || ""))
        : true,
    )
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
    .map((job) => ({ ...job, props: undefined }));
  return rows;
}

ensureDirs();
loadPersistedJobs();

module.exports = {
  createFastGpuJob,
  getFastGpuJob,
  listFastGpuJobs,
};
