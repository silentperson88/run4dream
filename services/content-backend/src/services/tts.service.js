const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const crypto = require("crypto");

const GENERATED_AUDIO_DIR = path.join(__dirname, "..", "..", "generated-audio");
const PIPER_BIN = process.env.PIPER_BIN || "piper";
const DEFAULT_TUNING = {
  speed: 1.0,
  noiseScale: 0.62,
  noiseW: 0.8,
  sentencePause: 0.28,
};
const DEFAULT_OPTIONS = {
  normalizeText: true,
  splitSentences: false,
};

const MODEL_CATALOG = {
  hi: {
    paratham: process.env.PIPER_MODEL_HI_PARATHAM || "C:\\piper\\models\\hindi\\hi_IN-pratham-medium.onnx",
  },
  en: {
    lessac: process.env.PIPER_MODEL_EN_LESSAC || "C:\\piper\\models\\english\\en_US-lessac-medium.onnx",
  },
};

function getPiperModelPath(language, model) {
  const modelPath = MODEL_CATALOG?.[language]?.[model];
  if (!modelPath) {
    throw new Error("Unsupported language/model selection");
  }

  return modelPath;
}

function generateFileName(language, model) {
  const suffix = crypto.randomUUID().slice(0, 8);
  return `${language}-${model}-${Date.now()}-${suffix}.wav`;
}

function ensureGeneratedDir() {
  fs.mkdirSync(GENERATED_AUDIO_DIR, { recursive: true });
}

function normalizeTextInput(text) {
  return text
    .replace(/\s+/g, " ")
    .replace(/₹/g, " rupees ")
    .replace(/\$/g, " dollars ")
    .replace(/%/g, " percent ")
    .trim();
}

function transliterateLatinWordToDevanagari(word) {
  let out = String(word || "").toLowerCase();
  const pairs = [
    ["tion", "शन"],
    ["sh", "श"],
    ["ch", "च"],
    ["th", "थ"],
    ["dh", "ध"],
    ["ph", "फ"],
    ["kh", "ख"],
    ["gh", "घ"],
    ["bh", "भ"],
    ["aa", "आ"],
    ["ee", "ई"],
    ["oo", "ऊ"],
    ["ai", "ऐ"],
    ["au", "औ"],
  ];
  pairs.forEach(([from, to]) => {
    out = out.replace(new RegExp(from, "g"), to);
  });

  const chars = {
    a: "अ",
    b: "ब",
    c: "क",
    d: "ड",
    e: "ए",
    f: "फ",
    g: "ग",
    h: "ह",
    i: "इ",
    j: "ज",
    k: "क",
    l: "ल",
    m: "म",
    n: "न",
    o: "ओ",
    p: "प",
    q: "क",
    r: "र",
    s: "स",
    t: "ट",
    u: "उ",
    v: "व",
    w: "व",
    x: "क्स",
    y: "य",
    z: "ज",
  };

  return out
    .split("")
    .map((ch) => chars[ch] || ch)
    .join("");
}

function normalizeTextByLanguage(text, language) {
  const base = normalizeTextInput(text);
  const lang = String(language || "").toLowerCase();
  if (lang !== "hi") return base;

  return base
    .replace(/&/g, " और ")
    .replace(/\b[A-Za-z][A-Za-z0-9'-]*\b/g, (word) => transliterateLatinWordToDevanagari(word));
}

function toSentences(text) {
  return text
    .split(/(?<=[.!?।])\s+/u)
    .map((part) => part.trim())
    .filter(Boolean);
}

function clampNumber(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function buildPiperArgs({ modelPath, outputFilePath, tuning = {} }) {
  const speed = clampNumber(tuning.speed, 0.75, 1.35, DEFAULT_TUNING.speed);
  const noiseScale = clampNumber(tuning.noiseScale, 0.1, 1.3, DEFAULT_TUNING.noiseScale);
  const noiseW = clampNumber(tuning.noiseW, 0.1, 1.3, DEFAULT_TUNING.noiseW);
  const sentencePause = clampNumber(tuning.sentencePause, 0, 1.2, DEFAULT_TUNING.sentencePause);

  // Piper expects length_scale where smaller means faster speech.
  const lengthScale = Number((1 / speed).toFixed(3));

  return [
    "--model",
    modelPath,
    "--output_file",
    outputFilePath,
    "--length_scale",
    String(lengthScale),
    "--noise_scale",
    String(noiseScale),
    "--noise_w",
    String(noiseW),
    "--sentence_silence",
    String(sentencePause),
  ];
}

function runPiper({ text, modelPath, outputFilePath, tuning }) {
  return new Promise((resolve, reject) => {
    const child = spawn(PIPER_BIN, buildPiperArgs({ modelPath, outputFilePath, tuning }), {
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (err) => {
      reject(err);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `Piper exited with code ${code}`));
        return;
      }

      resolve();
    });

    child.stdin.write(text, "utf8");
    child.stdin.end();
  });
}

function getWavDurationSec(filePath) {
  const buf = fs.readFileSync(filePath);
  if (buf.length < 44) return 0;

  const riff = buf.toString("ascii", 0, 4);
  const wave = buf.toString("ascii", 8, 12);
  if (riff !== "RIFF" || wave !== "WAVE") return 0;

  const sampleRate = buf.readUInt32LE(24);
  const numChannels = buf.readUInt16LE(22);
  const bitsPerSample = buf.readUInt16LE(34);
  const dataSize = buf.readUInt32LE(40);

  if (!sampleRate || !numChannels || !bitsPerSample || !dataSize) return 0;
  const bytesPerSecond = sampleRate * numChannels * (bitsPerSample / 8);
  if (!bytesPerSecond) return 0;

  return Number((dataSize / bytesPerSecond).toFixed(3));
}

async function generateAudio({ text, language, model, tuning, options }) {
  ensureGeneratedDir();

  const modelPath = getPiperModelPath(language, model);
  if (!fs.existsSync(modelPath)) {
    throw new Error(`Model file not found: ${modelPath}`);
  }

  const appliedOptions = {
    normalizeText: options?.normalizeText !== false,
    splitSentences: options?.splitSentences === true,
  };

  const preparedText = appliedOptions.normalizeText ? normalizeTextByLanguage(text, language) : text;
  const synthesisText = appliedOptions.splitSentences ? toSentences(preparedText).join("\n") : preparedText;

  const fileName = generateFileName(language, model);
  const outputFilePath = path.join(GENERATED_AUDIO_DIR, fileName);

  await runPiper({ text: synthesisText, modelPath, outputFilePath, tuning });
  if (!fs.existsSync(outputFilePath)) {
    throw new Error(`Piper completed but file not found at: ${outputFilePath}`);
  }

  const appliedTuning = {
    speed: clampNumber(tuning?.speed, 0.75, 1.35, DEFAULT_TUNING.speed),
    noiseScale: clampNumber(tuning?.noiseScale, 0.1, 1.3, DEFAULT_TUNING.noiseScale),
    noiseW: clampNumber(tuning?.noiseW, 0.1, 1.3, DEFAULT_TUNING.noiseW),
    sentencePause: clampNumber(tuning?.sentencePause, 0, 1.2, DEFAULT_TUNING.sentencePause),
  };

  return {
    fileName,
    filePath: outputFilePath,
    durationSec: getWavDurationSec(outputFilePath),
    modelPath,
    tuning: appliedTuning,
    options: appliedOptions,
  };
}

function getGeneratedAudioPath(fileName) {
  const safeName = path.basename(fileName || "");
  if (!safeName || safeName !== fileName) {
    throw new Error("Invalid file name");
  }

  return path.join(GENERATED_AUDIO_DIR, safeName);
}

module.exports = {
  generateAudio,
  getGeneratedAudioPath,
  MODEL_CATALOG,
  DEFAULT_TUNING,
  DEFAULT_OPTIONS,
};
