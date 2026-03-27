const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { getPool, ensureSchema } = require("../db/newsIngest.db");

const AUDIO_TOOLS_ROOT = path.join(__dirname, "..", "..", "generated-audio-tools");
const AUDIO_TOOLS_INPUT_DIR = path.join(AUDIO_TOOLS_ROOT, "input");
const AUDIO_TOOLS_OUTPUT_DIR = path.join(AUDIO_TOOLS_ROOT, "output");

const DEFAULT_OPTIONS = {
  preset: "original",
  outputFormat: "mp3",
  volumeDb: 0,
  speed: 1,
  pitchSemitones: 0,
  normalize: false,
  compress: false,
  noiseType: "none",
  noiseLevelDb: -30,
  reverb: false,
  reverbAmount: 0.25,
  echo: false,
  echoDelayMs: 110,
  echoDecay: 0.18,
  lowpassHz: 0,
  highpassHz: 0,
  fadeInSec: 0,
  fadeOutSec: 0,
  reverse: false,
};

function normalizePresetName(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 80);
}

const PRESET_MAP = {
  original: {},
  broadcast: {
    normalize: true,
    compress: true,
    lowpassHz: 12000,
    highpassHz: 80,
    volumeDb: 1,
  },
  warm_room: {
    normalize: true,
    reverb: true,
    reverbAmount: 0.25,
    noiseType: "room",
    noiseLevelDb: -36,
  },
  radio: {
    compress: true,
    highpassHz: 250,
    lowpassHz: 3800,
    noiseType: "hiss",
    noiseLevelDb: -30,
  },
  cinematic: {
    normalize: true,
    compress: true,
    reverb: true,
    echo: true,
    echoDelayMs: 140,
    echoDecay: 0.2,
    volumeDb: 2,
  },
  lofi: {
    lowpassHz: 7000,
    noiseType: "vinyl",
    noiseLevelDb: -32,
    echo: true,
    echoDelayMs: 95,
    echoDecay: 0.12,
  },
  noisy_tv: {
    normalize: true,
    noiseType: "white",
    noiseLevelDb: -24,
    lowpassHz: 5000,
  },
  clean: {
    normalize: true,
    compress: true,
  },
};

const NOISE_COLOR_MAP = {
  room: "pink",
  hiss: "white",
  white: "white",
  vinyl: "brown",
};

function ensureDirs() {
  fs.mkdirSync(AUDIO_TOOLS_INPUT_DIR, { recursive: true });
  fs.mkdirSync(AUDIO_TOOLS_OUTPUT_DIR, { recursive: true });
}

function sanitizeFileName(name) {
  return String(name || "")
    .replace(/[^\w.\-]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_+|_+$/g, "");
}

function slugify(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60) || "audio";
}

function parseDataUrl(dataUrl) {
  const raw = String(dataUrl || "");
  const match = raw.match(/^data:([^;]+);base64,(.+)$/);
  if (!match) throw new Error("audioDataUrl must be a valid base64 audio data URL");
  return { mime: match[1], data: Buffer.from(match[2], "base64") };
}

async function fetchBinary(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to download audio source");
  const arrayBuffer = await res.arrayBuffer();
  const mime = res.headers.get("content-type") || "application/octet-stream";
  return { mime, data: Buffer.from(arrayBuffer) };
}

function extFromMime(mime) {
  const lower = String(mime || "").toLowerCase();
  if (lower.includes("mpeg") || lower.includes("mp3")) return ".mp3";
  if (lower.includes("wav")) return ".wav";
  if (lower.includes("ogg")) return ".ogg";
  if (lower.includes("webm")) return ".webm";
  if (lower.includes("aac")) return ".aac";
  if (lower.includes("m4a") || lower.includes("mp4")) return ".m4a";
  return ".wav";
}

function outputExtFromFormat(format) {
  return String(format || "mp3").toLowerCase() === "wav" ? ".wav" : ".mp3";
}

function contentTypeFromExt(ext) {
  const lower = String(ext || "").toLowerCase();
  if (lower === ".mp3") return "audio/mpeg";
  if (lower === ".wav") return "audio/wav";
  if (lower === ".ogg") return "audio/ogg";
  if (lower === ".aac") return "audio/aac";
  if (lower === ".m4a") return "audio/mp4";
  return "application/octet-stream";
}

function runCommand(command, args, stdinText = "") {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stderr = "";
    let stdout = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr || stdout || `${command} exited with code ${code}`));
        return;
      }
      resolve({ stdout, stderr });
    });
    if (stdinText) {
      child.stdin.write(stdinText, "utf8");
    }
    child.stdin.end();
  });
}

async function probeAudio(filePath) {
  const args = [
    "-v",
    "error",
    "-select_streams",
    "a:0",
    "-show_entries",
    "stream=sample_rate,channels",
    "-show_entries",
    "format=duration",
    "-of",
    "json",
    filePath,
  ];
  const { stdout } = await runCommand("ffprobe", args);
  try {
    const parsed = JSON.parse(stdout || "{}");
    const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] || {} : {};
    const format = parsed?.format || {};
    return {
      sampleRate: Number(stream?.sample_rate || 44100) || 44100,
      channels: Number(stream?.channels || 2) || 2,
      durationSec: Number(format?.duration || 0) || 0,
    };
  } catch (_) {
    return { sampleRate: 44100, channels: 2, durationSec: 0 };
  }
}

function buildAtempoChain(factor) {
  let safeFactor = Number(factor);
  if (!Number.isFinite(safeFactor) || safeFactor <= 0) return [];
  const chain = [];
  while (safeFactor > 2) {
    chain.push(2);
    safeFactor /= 2;
  }
  while (safeFactor < 0.5) {
    chain.push(0.5);
    safeFactor /= 0.5;
  }
  if (Math.abs(safeFactor - 1) > 0.0001) {
    chain.push(Number(safeFactor.toFixed(6)));
  }
  return chain.map((value) => `atempo=${value}`);
}

function clampNumber(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function normalizeOptions(inputOptions = {}) {
  const presetName = String(inputOptions.preset || "original").toLowerCase();
  const preset = PRESET_MAP[presetName] || PRESET_MAP.original;
  return {
    ...DEFAULT_OPTIONS,
    ...preset,
    ...inputOptions,
    preset: presetName,
    outputFormat: String(inputOptions.outputFormat || DEFAULT_OPTIONS.outputFormat).toLowerCase() === "wav" ? "wav" : "mp3",
    noiseType: String(inputOptions.noiseType || DEFAULT_OPTIONS.noiseType).toLowerCase(),
    volumeDb: clampNumber(inputOptions.volumeDb ?? preset.volumeDb ?? DEFAULT_OPTIONS.volumeDb, -24, 24, DEFAULT_OPTIONS.volumeDb),
    speed: clampNumber(inputOptions.speed ?? preset.speed ?? DEFAULT_OPTIONS.speed, 0.5, 1.75, DEFAULT_OPTIONS.speed),
    pitchSemitones: clampNumber(
      inputOptions.pitchSemitones ?? preset.pitchSemitones ?? DEFAULT_OPTIONS.pitchSemitones,
      -12,
      12,
      DEFAULT_OPTIONS.pitchSemitones,
    ),
    noiseLevelDb: clampNumber(
      inputOptions.noiseLevelDb ?? preset.noiseLevelDb ?? DEFAULT_OPTIONS.noiseLevelDb,
      -60,
      0,
      DEFAULT_OPTIONS.noiseLevelDb,
    ),
    reverbAmount: clampNumber(
      inputOptions.reverbAmount ?? preset.reverbAmount ?? DEFAULT_OPTIONS.reverbAmount,
      0,
      1,
      DEFAULT_OPTIONS.reverbAmount,
    ),
    echoDelayMs: clampNumber(
      inputOptions.echoDelayMs ?? preset.echoDelayMs ?? DEFAULT_OPTIONS.echoDelayMs,
      20,
      800,
      DEFAULT_OPTIONS.echoDelayMs,
    ),
    echoDecay: clampNumber(
      inputOptions.echoDecay ?? preset.echoDecay ?? DEFAULT_OPTIONS.echoDecay,
      0.05,
      0.95,
      DEFAULT_OPTIONS.echoDecay,
    ),
    lowpassHz: clampNumber(
      inputOptions.lowpassHz ?? preset.lowpassHz ?? DEFAULT_OPTIONS.lowpassHz,
      0,
      20000,
      DEFAULT_OPTIONS.lowpassHz,
    ),
    highpassHz: clampNumber(
      inputOptions.highpassHz ?? preset.highpassHz ?? DEFAULT_OPTIONS.highpassHz,
      0,
      20000,
      DEFAULT_OPTIONS.highpassHz,
    ),
    fadeInSec: clampNumber(inputOptions.fadeInSec ?? DEFAULT_OPTIONS.fadeInSec, 0, 30, DEFAULT_OPTIONS.fadeInSec),
    fadeOutSec: clampNumber(inputOptions.fadeOutSec ?? DEFAULT_OPTIONS.fadeOutSec, 0, 30, DEFAULT_OPTIONS.fadeOutSec),
    normalize: Boolean(inputOptions.normalize ?? preset.normalize ?? DEFAULT_OPTIONS.normalize),
    compress: Boolean(inputOptions.compress ?? preset.compress ?? DEFAULT_OPTIONS.compress),
    reverb: Boolean(inputOptions.reverb ?? preset.reverb ?? DEFAULT_OPTIONS.reverb),
    echo: Boolean(inputOptions.echo ?? preset.echo ?? DEFAULT_OPTIONS.echo),
    reverse: Boolean(inputOptions.reverse ?? DEFAULT_OPTIONS.reverse),
  };
}

async function resolveInputAudio({ audioDataUrl, sourceUrl, fileName }) {
  ensureDirs();
  const safeBase = sanitizeFileName(path.basename(fileName || "uploaded-audio"));
  if (audioDataUrl) {
    const { mime, data } = parseDataUrl(audioDataUrl);
    const ext = extFromMime(mime);
    const inputFileName = safeBase.toLowerCase().endsWith(ext) ? safeBase : `${safeBase}${ext}`;
    const inputPath = path.join(AUDIO_TOOLS_INPUT_DIR, `${Date.now()}-${crypto.randomUUID().slice(0, 8)}-${inputFileName}`);
    fs.writeFileSync(inputPath, data);
    return { inputPath, originalBaseName: safeBase, cleanup: true };
  }

  if (sourceUrl) {
    const { mime, data } = await fetchBinary(sourceUrl);
    const ext = extFromMime(mime);
    const parsedName = safeBase.toLowerCase().endsWith(ext) ? safeBase : `${safeBase}${ext}`;
    const inputPath = path.join(AUDIO_TOOLS_INPUT_DIR, `${Date.now()}-${crypto.randomUUID().slice(0, 8)}-${parsedName}`);
    fs.writeFileSync(inputPath, data);
    return { inputPath, originalBaseName: safeBase, cleanup: true };
  }

  throw new Error("audioDataUrl or sourceUrl is required");
}

function buildPreMixFilters({ options, sampleRate }) {
  const filters = [];
  const pitchFactor = Math.pow(2, Number(options.pitchSemitones || 0) / 12);
  const atempoFactor = Number(options.speed || 1) / pitchFactor;

  if (options.reverse) {
    filters.push("areverse");
  }

  if (Math.abs(Number(options.pitchSemitones || 0)) > 0.001 || Math.abs(Number(options.speed || 1) - 1) > 0.001) {
    filters.push(`asetrate=${Math.max(8000, Math.round(sampleRate * pitchFactor))}`);
    filters.push(`aresample=${sampleRate}`);
    filters.push(...buildAtempoChain(atempoFactor));
  }

  if (Math.abs(Number(options.volumeDb || 0)) > 0.001) {
    filters.push(`volume=${Number(options.volumeDb).toFixed(2)}dB`);
  }

  if (Number(options.highpassHz || 0) > 0) {
    filters.push(`highpass=f=${Math.round(Number(options.highpassHz))}`);
  }

  if (Number(options.lowpassHz || 0) > 0) {
    filters.push(`lowpass=f=${Math.round(Number(options.lowpassHz))}`);
  }

  if (options.compress) {
    filters.push("acompressor=threshold=-18dB:ratio=3:attack=20:release=250");
  }

  if (options.reverb) {
    const amount = clampNumber(options.reverbAmount, 0, 1, 0.25);
    const delay1 = Math.max(30, Math.round(55 + amount * 70));
    const delay2 = Math.max(60, Math.round(95 + amount * 120));
    const decay1 = Number((0.1 + amount * 0.25).toFixed(3));
    const decay2 = Number((0.08 + amount * 0.18).toFixed(3));
    filters.push(`aecho=0.8:0.9:${delay1}|${delay2}:${decay1}|${decay2}`);
  }

  if (options.echo) {
    const delay = Math.round(Number(options.echoDelayMs || 110));
    const decay = clampNumber(options.echoDecay, 0.05, 0.95, 0.18);
    filters.push(`aecho=0.8:0.9:${delay}|${Math.max(delay * 2, delay + 60)}:${decay}|${Number((decay * 0.55).toFixed(3))}`);
  }

  return filters;
}

function buildPostMixFilters({ options, durationSec }) {
  const filters = [];
  if (options.normalize) {
    filters.push("loudnorm=I=-16:TP=-1.5:LRA=11:print_format=summary");
  }

  const fadeInSec = clampNumber(options.fadeInSec, 0, 30, 0);
  if (fadeInSec > 0) {
    filters.push(`afade=t=in:ss=0:d=${fadeInSec}`);
  }

  const fadeOutSec = clampNumber(options.fadeOutSec, 0, 30, 0);
  if (fadeOutSec > 0 && durationSec > 0) {
    const startSec = Math.max(0, durationSec - fadeOutSec);
    filters.push(`afade=t=out:st=${startSec.toFixed(3)}:d=${fadeOutSec}`);
  }

  return filters;
}

function buildNoiseMixFilter({ options, sampleRate, durationSec }) {
  const noiseType = String(options.noiseType || "none").toLowerCase();
  if (!noiseType || noiseType === "none") return null;

  const color = NOISE_COLOR_MAP[noiseType] || "white";
  const noiseVolumeDb = clampNumber(options.noiseLevelDb, -60, 0, -30);
  const noiseDuration = durationSec > 0 ? durationSec : 600;

  return `anoisesrc=color=${color}:duration=${noiseDuration.toFixed(3)}:sample_rate=${sampleRate},volume=${noiseVolumeDb}dB[noise]`;
}

function buildFilterComplex({ options, sampleRate, durationSec }) {
  const preMixFilters = buildPreMixFilters({ options, sampleRate });
  const postMixFilters = buildPostMixFilters({ options, durationSec });
  const noiseFilter = buildNoiseMixFilter({ options, sampleRate, durationSec });

  const preMixChain = preMixFilters.length ? `${preMixFilters.join(",")}` : "anull";
  const postMixChain = postMixFilters.length ? `${postMixFilters.join(",")}` : "";

  if (noiseFilter) {
    const mixTarget = postMixChain ? "[mixed]" : "[mixed]";
    const postPart = postMixChain ? `${mixTarget}${postMixChain}[out]` : `[mixed]anull[out]`;
    return `[0:a]${preMixChain}[voice];${noiseFilter};[voice][noise]amix=inputs=2:duration=first:dropout_transition=0[mixed];${postPart}`;
  }

  const postPart = postMixChain ? `[voice]${postMixChain}[out]` : `[voice]anull[out]`;
  return `[0:a]${preMixChain}[voice];${postPart}`;
}

async function processAudioModification({
  audioDataUrl,
  sourceUrl,
  fileName,
  preset = "original",
  outputFormat = "mp3",
  options = {},
}) {
  ensureDirs();
  const input = await resolveInputAudio({ audioDataUrl, sourceUrl, fileName });
  const inputPath = input.inputPath;
  const normalizedOptions = normalizeOptions({ ...options, preset, outputFormat });
  const meta = await probeAudio(inputPath);
  const outputExt = outputExtFromFormat(normalizedOptions.outputFormat);
  const originalBaseName = slugify(path.parse(fileName || input.originalBaseName || "audio").name);
  const suffix = crypto.randomUUID().slice(0, 8);
  const outputFileName = `${Date.now()}-${originalBaseName}-${normalizedOptions.preset}-${suffix}${outputExt}`;
  const outputPath = path.join(AUDIO_TOOLS_OUTPUT_DIR, outputFileName);

  const filterComplex = buildFilterComplex({
    options: normalizedOptions,
    sampleRate: meta.sampleRate,
    durationSec: meta.durationSec,
  });

  const args = [
    "-y",
    "-i",
    inputPath,
    "-filter_complex",
    filterComplex,
    "-map",
    "[out]",
  ];

  if (outputExt === ".wav") {
    args.push("-c:a", "pcm_s16le");
  } else {
    args.push("-c:a", "libmp3lame", "-q:a", "2");
  }

  args.push(outputPath);

  await runCommand("ffmpeg", args);

  if (!fs.existsSync(outputPath)) {
    throw new Error("Audio processing completed but output file was not created");
  }

  try {
    if (input.cleanup && fs.existsSync(inputPath)) {
      fs.unlinkSync(inputPath);
    }
  } catch (_) {
    // Ignore cleanup errors.
  }

  const relativeUrl = `content/news-content/audio-tools/generated/${outputFileName}`;
  return {
    fileName: outputFileName,
    filePath: outputPath,
    audioUrl: relativeUrl,
    outputFormat: normalizedOptions.outputFormat,
    durationSec: meta.durationSec,
    sampleRate: meta.sampleRate,
    originalFileName: path.basename(fileName || input.originalBaseName || "audio"),
    options: normalizedOptions,
  };
}

function getProcessedAudioPath(fileName) {
  const safeName = path.basename(String(fileName || ""));
  if (!safeName || safeName !== String(fileName || "")) {
    throw new Error("Invalid audio file name");
  }
  return path.join(AUDIO_TOOLS_OUTPUT_DIR, safeName);
}

function getProcessedAudioContentType(fileName) {
  const ext = path.extname(String(fileName || "")).toLowerCase();
  return contentTypeFromExt(ext);
}

async function savePreset({ userId, presetName, presetConfig }) {
  await ensureSchema();
  const name = normalizePresetName(presetName);
  if (!name) throw new Error("presetName is required");
  if (!presetConfig || typeof presetConfig !== "object") {
    throw new Error("presetConfig must be an object");
  }

  const db = getPool();
  const res = await db.query(
    `
      INSERT INTO audio_tools_presets (user_id, preset_name, preset_config)
      VALUES ($1, $2, $3::jsonb)
      ON CONFLICT (user_id, preset_name)
      DO UPDATE SET preset_config = EXCLUDED.preset_config, updated_at = NOW()
      RETURNING id, user_id, preset_name, preset_config, created_at, updated_at
    `,
    [Number(userId), name, JSON.stringify(presetConfig)],
  );
  return res.rows?.[0] || null;
}

async function listPresets({ userId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT id, user_id, preset_name, preset_config, created_at, updated_at
      FROM audio_tools_presets
      WHERE user_id = $1
      ORDER BY updated_at DESC, created_at DESC
    `,
    [Number(userId)],
  );
  return res.rows || [];
}

async function getPreset({ userId, id }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT id, user_id, preset_name, preset_config, created_at, updated_at
      FROM audio_tools_presets
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [Number(userId), Number(id)],
  );
  return res.rows?.[0] || null;
}

module.exports = {
  processAudioModification,
  getProcessedAudioPath,
  getProcessedAudioContentType,
  savePreset,
  listPresets,
  getPreset,
};
