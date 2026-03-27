const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { chatWithOllama } = require("./ollama.service");
const {
  getNewsItem,
  updateNewsApproachData,
  resetNewsApproachData,
  getMatchedNewsForBatch,
  createNewsApproachJob,
  updateNewsApproachJob,
  getNewsApproachJob,
  requestStopNewsApproachJob,
} = require("./newsIngest.service");
const ttsService = require("./tts.service");

const batchJobs = new Map();
const DEFAULT_BATCH_GAP_MS = Number(process.env.NEWS_APPROACH_BATCH_GAP_MS || 250);
const MAX_BATCH_ITEMS = Number(process.env.NEWS_APPROACH_BATCH_MAX_ITEMS || 5000);
const STALE_JOB_MS = Number(process.env.NEWS_APPROACH_STALE_JOB_MS || 10 * 60 * 1000);
const NEWS_APPROACH_ROOT_DIR = path.join(
  __dirname,
  "..",
  "..",
  "generated-news-content",
  "new-approach",
);
const NEWS_APPROACH_AUDIO_DIR = path.join(NEWS_APPROACH_ROOT_DIR, "audio");

function clean(value) {
  return String(value || "").trim();
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}

function ensureDirs() {
  fs.mkdirSync(NEWS_APPROACH_AUDIO_DIR, { recursive: true });
}

function sleep(ms) {
  const delay = Number(ms || 0);
  if (!Number.isFinite(delay) || delay <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, delay));
}

function normalizeImportantPoints(rawText, maxPoints) {
  const source = clean(rawText);
  if (!source) return "";

  const lines = source
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^[-*\u2022\d.)\]]+\s*/, "").trim())
    .filter(Boolean);

  let points = lines;
  if (!points.length) {
    points = source
      .split(/(?<=[.!?])\s+/)
      .map((line) => line.trim())
      .filter(Boolean);
  }

  const finalPoints = Number.isFinite(maxPoints) ? points.slice(0, maxPoints) : points;
  return finalPoints.map((point, index) => `${index + 1}. ${point}`).join("\n");
}

function sanitizeGeneratedScript(rawText) {
  const lines = String(rawText || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const blockedLine = (line) => {
    const v = line.toLowerCase();
    return (
      v.startsWith("okay, here's") ||
      v.startsWith("here is") ||
      v.startsWith("here's") ||
      v.includes("youtube news presenter script") ||
      v.includes("based on the provided points")
    );
  };

  const cleanedLines = lines.filter((line) => !blockedLine(line));
  let text = cleanedLines.join(" ").replace(/\s+/g, " ").trim();

  text = text
    .replace(/^(hi|hello|hey|good morning|good evening|namaste|namaskar)[\s,!.:-]*/i, "")
    .replace(/(thanks for watching|thank you for watching|thank you|dhanyavaad|dhanyavad|shukriya)[\s,!.:-]*$/i, "")
    .trim();

  return text;
}

function extractJsonArray(rawText) {
  if (Array.isArray(rawText)) return rawText;
  const raw = String(rawText || "").trim();
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed;
  } catch (_) {
    // try extracting first JSON array block
  }
  const m = raw.match(/\[[\s\S]*\]/);
  if (m?.[0]) {
    try {
      const parsed = JSON.parse(m[0]);
      if (Array.isArray(parsed)) return parsed;
    } catch (_) {
      // ignore
    }
  }
  return raw
    .split(/\r?\n|,/)
    .map((item) => item.replace(/^[-*\d.)\s]+/, "").trim())
    .filter(Boolean);
}

function extractJsonObject(rawText) {
  const raw = String(rawText || "").trim();
  if (!raw) return null;

  const sanitize = (value) =>
    String(value || "")
      .replace(/```json\s*,?/gi, "")
      .replace(/```/g, "")
      .trim();

  const tryParseObject = (value) => {
    const text = String(value || "").trim();
    if (!text) return null;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    } catch (_) {
      // ignore
    }
    return null;
  };

  const normalized = sanitize(raw);

  const direct = tryParseObject(normalized);
  if (direct) return direct;

  // Try to parse first JSON object block and repair trailing commas.
  const objectMatch = normalized.match(/\{[\s\S]*\}/);
  if (objectMatch?.[0]) {
    const block = objectMatch[0];
    const repaired = block.replace(/,\s*([}\]])/g, "$1");
    const repairedParsed = tryParseObject(repaired);
    if (repairedParsed) return repairedParsed;
  }

  // Fallback: extract arrays from raw text using regex keys.
  const extractList = (key) => {
    const re = new RegExp(`"${key}"\\s*:\\s*\\[([\\s\\S]*?)\\]`, "i");
    const m = normalized.match(re);
    if (!m?.[1]) return [];
    return m[1]
      .split(",")
      .map((item) => item.replace(/^["'\s]+|["'\s]+$/g, "").trim())
      .filter(Boolean);
  };

  const positive = extractList("positive");
  const negative = extractList("negative");
  const important = extractList("important");
  const neutral = extractList("neutral");
  if (positive.length || negative.length || important.length || neutral.length) {
    return { positive, negative, important: important.length ? important : neutral };
  }

  return null;
}

function toPublicJob(job) {
  return {
    id: job.id,
    status: job.status,
    date: job.date,
    category: job.category,
    forcedMatchStatus: "matched",
    model: job.model,
    total: job.total,
    processed: job.processed,
    success: job.success,
    failed: job.failed,
    skipped: job.skipped,
    progress: job.total > 0 ? Number(((job.processed / job.total) * 100).toFixed(2)) : 100,
    currentNewsId: job.currentNewsId,
    currentHeadline: job.currentHeadline,
    gapMs: job.gapMs,
    errors: Array.isArray(job.errors) ? job.errors.slice(0, 20) : [],
    cancelRequested: Boolean(job.cancelRequested),
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    updatedAt: job.updatedAt || null,
  };
}

async function persistJob(job) {
  try {
    const updated = await updateNewsApproachJob({
      userId: Number(job.userId),
      jobId: String(job.id),
      patch: {
        status: job.status,
        category: job.category,
        model: job.model,
        total: job.total,
        processed: job.processed,
        success: job.success,
        failed: job.failed,
        skipped: job.skipped,
        currentNewsId: job.currentNewsId,
        currentHeadline: job.currentHeadline,
        gapMs: job.gapMs,
        cancelRequested: Boolean(job.cancelRequested),
        errors: job.errors,
        startedAt: job.startedAt,
        finishedAt: job.finishedAt,
      },
    });
    if (updated?.updatedAt) {
      job.updatedAt = updated.updatedAt;
    }
  } catch (_) {
    // best effort
  }
}

async function summarizeNewsPdf({ userId, newsId, model }) {
  const item = await getNewsItem({ userId, id: newsId });
  const sourceText = clean(item?.pdfText) || clean(item?.headline) || clean(item?.company);
  if (!sourceText) {
    throw new Error("No announcement text available for this news");
  }

  const prompt = [
    "Analyze this announcement text and give short summary in points.",
    "Write output in English only.",
    "Keep each point short and factual.",
    "",
    `Title: ${clean(item?.headline)}`,
    `Company: ${clean(item?.company)}`,
    "",
    "Text:",
    sourceText.slice(0, 25000),
  ].join("\n");

  const res = await chatWithOllama({
    model,
    text: prompt,
    options: {
      temperature: 0.2,
    },
  });

  return {
    newsId: Number(newsId),
    model: res?.model || clean(model),
    summaryPoints: clean(res?.response),
    sourceHeadline: clean(item?.headline),
  };
}

async function extractImportantPoints({ userId, newsId, summary, model }) {
  const source = clean(summary);
  if (!source) throw new Error("summary is required");

  const prompt = [
    "Task: Extract major important points for this news from the text below.",
    "Write output in English only.",
    "Output rules:",
    "1) Return concise point-wise output.",
    "2) Keep each point factual and useful for news reading.",
    "3) Focus on event, impact, key numbers, and timeline if available.",
    "4) Do not add filler, intro, or conclusion.",
    "",
    "Input text:",
    source.slice(0, 12000),
  ].join("\n");

  const res = await chatWithOllama({
    model,
    text: prompt,
    options: {
      temperature: 0.1,
    },
  });

  const importantPoints = normalizeImportantPoints(res?.response);
  if (Number(newsId) > 0 && Number(userId) > 0) {
    await updateNewsApproachData({
      userId: Number(userId),
      id: Number(newsId),
      importantPointsText: importantPoints,
    });
  }

  return {
    model: res?.model || clean(model),
    importantPoints,
  };
}

async function generateScriptFromPoints({ userId, newsId, points, model, language }) {
  const source = clean(points);
  if (!source) throw new Error("points is required");

  const lang = clean(language).toLowerCase() === "hindi" ? "hindi" : "english";
  const languageRule =
    lang === "hindi"
      ? "Write in Hindi only using Devanagari script. Do not use English words."
      : "Write in English only.";

  const prompt = [
    "Create a short YouTube news script from these points.",
    languageRule,
    "Use neutral newsroom tone.",
    "Cover only major facts.",
    "Return one paragraph only.",
    "Do not add greeting.",
    "Do not add thank-you or closing line.",
    "Do not add any intro like 'Here is the script'.",
    "",
    "Points:",
    source.slice(0, 16000),
  ].join("\n");

  const res = await chatWithOllama({
    model,
    text: prompt,
    options: {
      temperature: 0.2,
    },
  });

  const script = sanitizeGeneratedScript(res?.response);
  if (Number(newsId) > 0 && Number(userId) > 0) {
    await updateNewsApproachData({
      userId: Number(userId),
      id: Number(newsId),
      ...(lang === "hindi" ? { scriptHindi: script } : { scriptEnglish: script }),
    });
  }

  return {
    model: res?.model || clean(model),
    language: lang,
    script,
  };
}

async function generateStructuredScriptFromPoints({
  userId,
  newsId,
  points,
  model,
  language,
  targetDurationSec,
  wordCountMin,
  wordCountMax,
}) {
  const source = clean(points);
  if (!source) throw new Error("points is required");

  const lang = clean(language).toLowerCase() === "hindi" ? "hindi" : "english";
  const durationSec = clampNumber(targetDurationSec, 60, 900, 240);
  let minWords = clampNumber(wordCountMin, 120, 5000, 0);
  let maxWords = clampNumber(wordCountMax, 150, 6000, 0);

  if (!minWords || !maxWords) {
    const baseMin = Math.round(durationSec * 1.6);
    const baseMax = Math.round(durationSec * 1.9);
    if (!minWords) minWords = clampNumber(baseMin, 120, 5000, baseMin);
    if (!maxWords) maxWords = clampNumber(baseMax, minWords, 6000, baseMax);
  }
  if (maxWords < minWords) maxWords = minWords;

  const languageRule =
    lang === "hindi"
      ? "Write in Hindi only using Devanagari script. Do not use English words."
      : "Write in English only.";

  const prompt = [
    "Act as a professional YouTube news script writer.",
    "Rewrite the following news points into an engaging YouTube video script.",
    "",
    "Requirements:",
    `- Language: ${lang === "hindi" ? "Hindi" : "English"}`,
    `- Video length: about ${Math.max(1, Math.round(durationSec / 60))} minutes`,
    `- Target word count: ${minWords}–${maxWords} words`,
    "- Do NOT copy sentences from the points.",
    "- Rewrite and simplify the information.",
    "- Use conversational language.",
    "- Make it suitable for voice narration.",
    languageRule,
    "",
    "Structure (follow this sequence):",
    "Hook",
    "News introduction",
    "Key details",
    "Why this news matters",
    "Future impact",
    "Ending question",
    "",
    "Return ONLY valid JSON with this exact shape:",
    '{ "hook": "string", "newsIntroduction": "string", "keyDetails": "string", "whyThisMatters": "string", "futureImpact": "string", "endingQuestion": "string", "fullScript": "string" }',
    "",
    "Rules:",
    "- Each section should be 1-4 sentences.",
    "- fullScript must be a single paragraph that concatenates the sections in order, with no headings.",
    "- No greetings, no sign-off.",
    "",
    "Points:",
    source.slice(0, 16000),
  ].join("\n");

  const res = await chatWithOllama({
    model,
    text: prompt,
    options: {
      temperature: 0.2,
    },
  });

  const parsed = extractJsonObject(res?.response) || {};
  const hook = clean(parsed.hook);
  const newsIntroduction = clean(parsed.newsIntroduction);
  const keyDetails = clean(parsed.keyDetails);
  const whyThisMatters = clean(parsed.whyThisMatters);
  const futureImpact = clean(parsed.futureImpact);
  const endingQuestion = clean(parsed.endingQuestion);
  const fullScript = clean(parsed.fullScript) || clean(
    [hook, newsIntroduction, keyDetails, whyThisMatters, futureImpact, endingQuestion]
      .filter(Boolean)
      .join(" "),
  );

  if (Number(newsId) > 0 && Number(userId) > 0) {
    await updateNewsApproachData({
      userId: Number(userId),
      id: Number(newsId),
      ...(lang === "hindi" ? { scriptHindi: fullScript } : { scriptEnglish: fullScript }),
    });
  }

  return {
    model: res?.model || clean(model),
    language: lang,
    durationSec,
    wordCountMin: minWords,
    wordCountMax: maxWords,
    hook,
    newsIntroduction,
    keyDetails,
    whyThisMatters,
    futureImpact,
    endingQuestion,
    fullScript,
  };
}

async function generateScriptAudio({ userId, newsId, script, language, model, tuning, options }) {
  const text = clean(script);
  if (!text) throw new Error("script is required");

  const lang = clean(language).toLowerCase() === "hindi" ? "hi" : "en";
  const fallbackModel = lang === "hi" ? "paratham" : "lessac";
  const selectedModel = clean(model).toLowerCase() || fallbackModel;
  const item = Number(newsId) > 0 ? await getNewsItem({ userId, id: newsId }) : null;
  const company = clean(item?.company);
  const ttsIntro = company
    ? lang === "hi"
      ? `कंपनी ${company} पर अपडेट। `
      : `${company} update. `
    : "";
  const ttsText = `${ttsIntro}${text}`.trim();

  const audio = await ttsService.generateAudio({
    text: ttsText,
    language: lang,
    model: selectedModel,
    tuning,
    options,
  });

  ensureDirs();
  const storedFileName = `news-${Number(newsId || 0)}-${Date.now()}-${audio.fileName}`;
  const targetPath = path.join(NEWS_APPROACH_AUDIO_DIR, storedFileName);
  try {
    fs.renameSync(audio.filePath, targetPath);
  } catch (_) {
    fs.copyFileSync(audio.filePath, targetPath);
    fs.unlinkSync(audio.filePath);
  }

  const audioUrl = `content/news-content/new-approach/assets/audio/${storedFileName}`;
  if (Number(newsId) > 0 && Number(userId) > 0) {
    await updateNewsApproachData({
      userId: Number(userId),
      id: Number(newsId),
      ...(lang === "hi" ? { scriptAudioHindi: audioUrl } : { scriptAudioEnglish: audioUrl }),
    });
  }

  return {
    language: lang === "hi" ? "hindi" : "english",
    model: selectedModel,
    audioFileName: storedFileName,
    audioUrl,
    durationSec: Number(audio?.durationSec || 0),
  };
}

async function extractHighlightTerms({ userId, newsId, script, language, model }) {
  const text = clean(script);
  if (!text) throw new Error("script is required");
  const lang = clean(language).toLowerCase() === "hindi" ? "hindi" : "english";
  const languageRule = lang === "hindi" ? "Output terms in Hindi (Devanagari)." : "Output terms in English.";

  const prompt = [
    "Task: Extract highlight words/phrases from this news paragraph and classify them into positive, negative, and important.",
    languageRule,
    "Return strictly JSON object only (no explanation):",
    '{"positive": ["..."], "negative": ["..."], "important": ["..."]}',
    "Rules:",
    "1) Include key numbers, percentages, prices, dates, and impactful nouns.",
    "2) Keep each term short (1-4 words).",
    "3) Put clearly favorable signals in positive.",
    "4) Put clearly unfavorable signals in negative.",
    "5) Put major factual highlights that are neither clearly positive nor negative into important.",
    "6) JSON only. No explanation text.",
    "",
    "Paragraph:",
    text.slice(0, 24000),
  ].join("\n");

  const res = await chatWithOllama({
    model,
    text: prompt,
    options: {
      temperature: 0.1,
    },
  });

  const parsedObj = extractJsonObject(res?.response);
  const fallbackTerms = parsedObj ? [] : extractJsonArray(res?.response);
  const positiveTerms = extractJsonArray(parsedObj?.positive)
    .map((item) => clean(item))
    .filter(Boolean)
    .slice(0, 25);
  const negativeTerms = extractJsonArray(parsedObj?.negative)
    .map((item) => clean(item))
    .filter(Boolean)
    .slice(0, 25);
  const importantTerms = extractJsonArray(parsedObj?.important || parsedObj?.neutral || fallbackTerms)
    .map((item) => clean(item))
    .filter(Boolean)
    .slice(0, 25);
  const terms = Array.from(new Set([...positiveTerms, ...negativeTerms, ...importantTerms])).slice(0, 50);

  if (Number(newsId) > 0 && Number(userId) > 0) {
    await updateNewsApproachData({
      userId: Number(userId),
      id: Number(newsId),
      highlightTerms: terms,
      highlightTermsPositive: positiveTerms,
      highlightTermsNegative: negativeTerms,
    });
  }

  return {
    language: lang,
    model: res?.model || clean(model),
    terms,
    positiveTerms,
    negativeTerms,
    importantTerms,
  };
}

function getNewsApproachAudioPath(fileName) {
  const safeName = path.basename(String(fileName || ""));
  if (!safeName || safeName !== fileName) {
    throw new Error("Invalid file name");
  }
  ensureDirs();
  return path.join(NEWS_APPROACH_AUDIO_DIR, safeName);
}

async function shouldStop(job) {
  if (job.cancelRequested) return true;
  const dbJob = await getNewsApproachJob({ userId: job.userId, jobId: job.id });
  if (dbJob?.cancelRequested) {
    job.cancelRequested = true;
    return true;
  }
  return false;
}

async function stopWithCurrentRowReset(job, rowId, rowTouched) {
  if (rowTouched && Number(rowId) > 0) {
    await resetNewsApproachData({ userId: Number(job.userId), id: Number(rowId) });
  }
  job.status = "stopped";
  job.finishedAt = new Date().toISOString();
  job.currentNewsId = null;
  job.currentHeadline = "";
  await persistJob(job);
}

async function runBatch(job) {
  try {
    for (let i = 0; i < job.items.length; i += 1) {
      const item = job.items[i];
      job.currentNewsId = item.id;
      job.currentHeadline = item.headline || item.company || "";
      await persistJob(job);
      let counted = false;
      let rowTouched = false;

      if (await shouldStop(job)) {
        await stopWithCurrentRowReset(job, item.id, false);
        return;
      }

      try {
        const existingPoints = clean(item.importantPointsText);
        const existingScript = clean(item.scriptEnglish);
        const existingAudio = clean(item.scriptAudioEnglish);
        const existingHighlightTerms = Array.isArray(item.highlightTerms)
          ? item.highlightTerms.map((term) => clean(term)).filter(Boolean)
          : [];
        const existingHighlightTermsPositive = Array.isArray(item.highlightTermsPositive)
          ? item.highlightTermsPositive.map((term) => clean(term)).filter(Boolean)
          : [];
        const existingHighlightTermsNegative = Array.isArray(item.highlightTermsNegative)
          ? item.highlightTermsNegative.map((term) => clean(term)).filter(Boolean)
          : [];
        const hasAnyHighlights =
          existingHighlightTerms.length > 0 ||
          existingHighlightTermsPositive.length > 0 ||
          existingHighlightTermsNegative.length > 0;

        if (existingPoints && existingScript && existingAudio && hasAnyHighlights) {
          job.skipped += 1;
          job.processed += 1;
          counted = true;
          await persistJob(job);
          continue;
        }

        let points = existingPoints;
        if (!points) {
          const summary = await summarizeNewsPdf({
            userId: job.userId,
            newsId: item.id,
            model: job.model,
          });
          await sleep(job.gapMs);
          const important = await extractImportantPoints({
            userId: job.userId,
            newsId: item.id,
            summary: summary.summaryPoints,
            model: job.model,
          });
          rowTouched = true;
          points = clean(important.importantPoints);
          await sleep(job.gapMs);
          if (await shouldStop(job)) {
            await stopWithCurrentRowReset(job, item.id, rowTouched);
            return;
          }
        }

        if (!existingScript && points) {
          const generated = await generateScriptFromPoints({
            userId: job.userId,
            newsId: item.id,
            points,
            model: job.model,
            language: "english",
          });
          rowTouched = true;
          item.scriptEnglish = clean(generated?.script);
          await sleep(job.gapMs);
          if (await shouldStop(job)) {
            await stopWithCurrentRowReset(job, item.id, rowTouched);
            return;
          }
        }

        const scriptForAudio = clean(item.scriptEnglish || existingScript);
        if (!hasAnyHighlights && scriptForAudio) {
          await extractHighlightTerms({
            userId: job.userId,
            newsId: item.id,
            script: scriptForAudio,
            language: "english",
            model: job.model,
          });
          rowTouched = true;
          await sleep(job.gapMs);
          if (await shouldStop(job)) {
            await stopWithCurrentRowReset(job, item.id, rowTouched);
            return;
          }
        }

        if (!existingAudio && scriptForAudio) {
          await generateScriptAudio({
            userId: job.userId,
            newsId: item.id,
            script: scriptForAudio,
            language: "english",
            model: "lessac",
          });
          rowTouched = true;
          await sleep(job.gapMs);
          if (await shouldStop(job)) {
            await stopWithCurrentRowReset(job, item.id, rowTouched);
            return;
          }
        }

        job.success += 1;
      } catch (err) {
        job.failed += 1;
        job.errors.push({
          newsId: item.id,
          headline: item.headline || item.company || "",
          message: err?.message || "Unknown error",
        });
      } finally {
        if (!counted) {
          job.processed += 1;
        }
        await persistJob(job);
      }
    }

    job.status = "completed";
    job.finishedAt = new Date().toISOString();
    job.currentNewsId = null;
    job.currentHeadline = "";
    await persistJob(job);
  } catch (err) {
    job.status = "failed";
    job.finishedAt = new Date().toISOString();
    job.errors.push({
      newsId: 0,
      headline: "",
      message: err?.message || "Batch failed unexpectedly",
    });
    await persistJob(job);
  } finally {
    batchJobs.delete(job.id);
  }
}

async function startBatchGeneration({ userId, date, category, model, gapMs }) {
  ensureDirs();
  const items = await getMatchedNewsForBatch({
    userId: Number(userId),
    date,
    category,
  });

  const cappedItems = items.slice(0, Math.max(1, MAX_BATCH_ITEMS));
  const job = {
    id: crypto.randomUUID(),
    userId: Number(userId),
    status: "running",
    date: clean(date),
    category: clean(category) || "all",
    model: clean(model),
    gapMs: Number.isFinite(Number(gapMs)) ? Math.max(0, Number(gapMs)) : Math.max(0, DEFAULT_BATCH_GAP_MS),
    total: cappedItems.length,
    processed: 0,
    success: 0,
    failed: 0,
    skipped: 0,
    currentNewsId: null,
    currentHeadline: "",
    errors: [],
    cancelRequested: false,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    items: cappedItems,
  };

  await createNewsApproachJob(job);
  batchJobs.set(job.id, job);
  await persistJob(job);
  void runBatch(job);
  return toPublicJob(job);
}

async function getBatchStatus({ userId, jobId }) {
  const id = clean(jobId);
  const inMemory = batchJobs.get(id);
  const job = inMemory || (await getNewsApproachJob({ userId: Number(userId), jobId: id }));
  if (!job || Number(job.userId) !== Number(userId)) {
    throw new Error("Batch job not found");
  }
  if (!inMemory && job.status === "running") {
    const last = new Date(job.updatedAt || job.startedAt || 0).getTime();
    if (Number.isFinite(last) && Date.now() - last > STALE_JOB_MS) {
      const updated = await updateNewsApproachJob({
        userId: Number(userId),
        jobId: id,
        patch: {
          status: "failed",
          finishedAt: new Date().toISOString(),
          errors: [
            ...(Array.isArray(job.errors) ? job.errors : []),
            {
              newsId: 0,
              headline: "",
              message: "Batch interrupted (service restart or stalled worker). Please start a new batch.",
            },
          ],
        },
      });
      return toPublicJob(updated || job);
    }
  }
  return toPublicJob(job);
}

async function stopBatchGeneration({ userId, jobId }) {
  const id = clean(jobId);
  const updated = await requestStopNewsApproachJob({
    userId: Number(userId),
    jobId: id,
  });
  if (!updated) throw new Error("Batch job not found");

  const running = batchJobs.get(id);
  if (running) {
    running.cancelRequested = true;
    running.status = running.status === "running" ? "stopping" : running.status;
    await persistJob(running);
    return toPublicJob(running);
  }
  return toPublicJob(updated);
}

module.exports = {
  summarizeNewsPdf,
  extractImportantPoints,
  generateScriptFromPoints,
  generateStructuredScriptFromPoints,
  generateScriptAudio,
  extractHighlightTerms,
  getNewsApproachAudioPath,
  startBatchGeneration,
  getBatchStatus,
  stopBatchGeneration,
};
