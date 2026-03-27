const { chatWithOllama } = require("./ollama.service");
const ttsService = require("./tts.service");

const DEFAULT_MAX_RETRIES = 2;
const SCENE_AUDIO_PADDING_SEC = 0.175;

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}

function safeTrim(value) {
  return String(value || "").trim();
}

function resolveModelLanguage(language) {
  const raw = safeTrim(language).toLowerCase();
  if (!raw) return "English";
  if (raw === "hi" || raw === "hindi") return "Hindi";
  if (raw === "en" || raw === "english") return "English";
  return safeTrim(language);
}

function splitTextBySentences(text) {
  return safeTrim(text)
    .split(/(?<=[.!?।])\s+/u)
    .map((s) => s.trim())
    .filter(Boolean);
}

function normalizeInfoType(infoType) {
  const type = safeTrim(infoType).toUpperCase();
  if (["GENERAL_CONTEXT", "SUBTOPIC_LIST", "CONTEXT_PLUS_SUBTOPICS"].includes(type)) {
    return type;
  }
  return "GENERAL_CONTEXT";
}

function normalizeSubtopics(inputSubtopics, extraInfo = "", infoType = "GENERAL_CONTEXT") {
  const mode = normalizeInfoType(infoType);
  const fromArray = Array.isArray(inputSubtopics)
    ? inputSubtopics.map((x) => safeTrim(x)).filter(Boolean)
    : [];
  if (fromArray.length) return fromArray.slice(0, 30);
  if (mode === "GENERAL_CONTEXT") return [];

  const lines = String(extraInfo || "")
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*[-*•\d.)]+\s*/, "").trim())
    .filter(Boolean);

  const likelyTopics = lines.filter((line) => {
    if (line.length < 3 || line.length > 140) return false;
    const wordCount = line.split(/\s+/).length;
    return wordCount >= 1 && wordCount <= 14;
  });

  return likelyTopics.slice(0, 30);
}

function containsHeadingKeyword(heading, keywords) {
  const h = safeTrim(heading).toLowerCase();
  return keywords.some((k) => h.includes(k));
}

function enforceSubtopicCoverage({ scenes, subtopics, language }) {
  const items = Array.isArray(scenes) ? [...scenes] : [];
  const subs = Array.isArray(subtopics) ? subtopics.map((s) => safeTrim(s)).filter(Boolean) : [];
  const lang = resolveModelLanguage(language).toLowerCase();
  if (!subs.length) return items;

  const introHeading = lang === "hindi" ? "परिचय" : "Introduction";
  const outroHeading = lang === "hindi" ? "समापन और धन्यवाद" : "Conclusion and Thank You";
  const introNarration =
    lang === "hindi"
      ? "आज के वीडियो में हम इन विषयों को आसान भाषा में समझेंगे।"
      : "In this video, we will quickly explore each subtopic in a clear and engaging way.";
  const outroNarration =
    lang === "hindi"
      ? "वीडियो देखने के लिए धन्यवाद। अपने विचार और अगले विषय कमेंट में साझा करें।"
      : "Thanks for watching. Share your thoughts and suggest the next topic in comments.";

  if (!items.some((s) => containsHeadingKeyword(s.heading, ["intro", "introduction", "परिचय", "opening"]))) {
    items.unshift({
      id: 0,
      heading: introHeading,
      narration: introNarration,
      onScreenText: introHeading,
      voiceStyle: "Clear neutral narration",
      durationSec: 6,
    });
  }

  const withSubtopics = [...items];
  subs.forEach((sub) => {
    const hasScene = withSubtopics.some((s) => {
      const heading = safeTrim(s.heading).toLowerCase();
      const narration = safeTrim(s.narration).toLowerCase();
      const probe = sub.toLowerCase();
      return heading.includes(probe) || narration.includes(probe);
    });

    if (!hasScene) {
      withSubtopics.push({
        id: 0,
        heading: sub,
        narration:
          lang === "hindi"
            ? `${sub} के बारे में मुख्य बातों को सरल तरीके से समझते हैं।`
            : `Now let's cover ${sub} with practical and easy-to-understand points.`,
        onScreenText: sub,
        voiceStyle: "Clear neutral narration",
        durationSec: 8,
      });
    }
  });

  if (!withSubtopics.some((s) => containsHeadingKeyword(s.heading, ["conclusion", "outro", "thank", "समापन", "धन्यवाद"]))) {
    withSubtopics.push({
      id: 0,
      heading: outroHeading,
      narration: outroNarration,
      onScreenText: outroHeading,
      voiceStyle: "Warm closing tone",
      durationSec: 6,
    });
  }

  return withSubtopics;
}

function buildFallbackScenesFromScript(script, preferredSceneCount, targetDurationSec) {
  const sentences = splitTextBySentences(script);
  const base = sentences.length ? sentences : [safeTrim(script)];
  const desired = clampNumber(preferredSceneCount, 2, 20, 8);
  const chunkSize = Math.max(1, Math.ceil(base.length / desired));

  const grouped = [];
  for (let i = 0; i < base.length; i += chunkSize) {
    const narration = base.slice(i, i + chunkSize).join(" ").trim();
    if (!narration) continue;
    grouped.push({
      heading: `Scene ${grouped.length + 1}`,
      narration,
      onScreenText: narration.slice(0, 90),
      voiceStyle: "Clear neutral narration",
      durationSec: 8,
    });
  }

  const minScenes = 2;
  if (grouped.length < minScenes) {
    let first = "";
    let second = "";

    if (base.length > 1) {
      const mid = Math.ceil(base.length / 2);
      first = base.slice(0, mid).join(" ").trim();
      second = base.slice(mid).join(" ").trim();
    } else {
      const words = safeTrim(script).split(/\s+/).filter(Boolean);
      const mid = Math.ceil(words.length / 2);
      first = words.slice(0, mid).join(" ").trim();
      second = words.slice(mid).join(" ").trim();
    }

    const rebuilt = [];
    if (first) rebuilt.push({ heading: "Scene 1", narration: first, onScreenText: first.slice(0, 90), voiceStyle: "Clear neutral narration", durationSec: 8 });
    if (second) rebuilt.push({ heading: "Scene 2", narration: second, onScreenText: second.slice(0, 90), voiceStyle: "Clear neutral narration", durationSec: 8 });
    return normalizeScenes(rebuilt, targetDurationSec, script, preferredSceneCount);
  }

  return grouped;
}

function stripCodeFence(text) {
  const raw = safeTrim(text);
  if (!raw) return raw;
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced?.[1]) return fenced[1].trim();
  return raw;
}

function extractJsonCandidate(text) {
  const direct = stripCodeFence(text);
  if (!direct) return "";

  if (direct.startsWith("{") || direct.startsWith("[")) return direct;

  const firstObj = direct.indexOf("{");
  const lastObj = direct.lastIndexOf("}");
  const firstArr = direct.indexOf("[");
  const lastArr = direct.lastIndexOf("]");

  const objectCandidate =
    firstObj >= 0 && lastObj > firstObj ? direct.slice(firstObj, lastObj + 1) : "";
  const arrayCandidate =
    firstArr >= 0 && lastArr > firstArr ? direct.slice(firstArr, lastArr + 1) : "";

  return arrayCandidate.length > objectCandidate.length ? arrayCandidate : objectCandidate;
}

function parseJsonFromModel(text) {
  const candidate = extractJsonCandidate(text);
  if (!candidate) {
    throw new Error("Model did not return valid JSON");
  }
  try {
    return JSON.parse(candidate);
  } catch (_) {
    const repaired = repairJsonControlChars(candidate);
    return JSON.parse(repaired);
  }
}

function repairJsonControlChars(jsonText) {
  const src = String(jsonText || "");
  let out = "";
  let inString = false;
  let escaped = false;

  for (let i = 0; i < src.length; i += 1) {
    const ch = src[i];
    const code = src.charCodeAt(i);

    if (inString) {
      if (escaped) {
        out += ch;
        escaped = false;
        continue;
      }

      if (ch === "\\") {
        out += ch;
        escaped = true;
        continue;
      }

      if (ch === '"') {
        out += ch;
        inString = false;
        continue;
      }

      if (ch === "\n") {
        out += "\\n";
        continue;
      }
      if (ch === "\r") {
        out += "\\r";
        continue;
      }
      if (ch === "\t") {
        out += "\\t";
        continue;
      }

      if (code >= 0 && code <= 31) {
        out += `\\u${code.toString(16).padStart(4, "0")}`;
        continue;
      }

      out += ch;
      continue;
    }

    if (ch === '"') {
      inString = true;
      out += ch;
      continue;
    }

    out += ch;
  }

  return out;
}

async function askModelForJson({ model, systemPrompt, userPrompt, maxRetries = DEFAULT_MAX_RETRIES }) {
  let lastErr = null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const res = await chatWithOllama({
        model,
        systemPrompt,
        text: userPrompt,
        format: "json",
        options: {
          temperature: 0.2,
        },
      });
      let parsed = null;
      try {
        parsed = parseJsonFromModel(res?.response || "");
      } catch (parseErr) {
        const repairPrompt = `
You will receive malformed JSON.
Return ONLY valid JSON, preserving keys and values as much as possible.

Malformed JSON:
${String(res?.response || "").slice(0, 25000)}
`.trim();
        const repaired = await chatWithOllama({
          model,
          systemPrompt: "You are a JSON repair utility. Output valid JSON only.",
          text: repairPrompt,
          format: "json",
          options: {
            temperature: 0,
          },
        });
        parsed = parseJsonFromModel(repaired?.response || "");
      }
      return { parsed, model: res?.model || model };
    } catch (err) {
      lastErr = err;
    }
  }

  throw new Error(lastErr?.message || "Failed to parse structured response from model");
}

function buildScriptGenerationPrompt(input) {
  const topic = safeTrim(input.topic);
  const context = safeTrim(input.context || input.extraInfo);
  const infoType = normalizeInfoType(input.extraInfoType);
  const subtopics = normalizeSubtopics(input.subtopics, context, infoType);
  const tone = safeTrim(input.tone) || "professional and engaging";
  const platform = safeTrim(input.platform) || "YouTube";
  const language = resolveModelLanguage(input.language);
  const targetDurationSec = clampNumber(input.targetDurationSec, 20, 900, 90);
  const targetWords = clampNumber((targetDurationSec * 2.2).toFixed(0), 60, 1800, 220);

  return `
Generate a long-form creator script package for this topic.

Topic: ${topic}
Extra context: ${context || "N/A"}
Extra info type: ${infoType}
Platform: ${platform}
Language: ${language}
Tone: ${tone}
Target duration (seconds): ${targetDurationSec}
Target word count approx: ${targetWords}
Subtopics to include (if provided):
${subtopics.length ? subtopics.map((s, i) => `${i + 1}. ${s}`).join("\n") : "None"}

Return ONLY valid JSON with this exact shape:
{
  "title": "string",
  "hook": "string",
  "summaryLong": "string",
  "videoScript": "string",
  "cta": "string",
  "estimatedWords": 220
}

Rules:
- summaryLong: 140-260 words
- videoScript: coherent narration for full video, no markdown
- write all fields in ${language} language
- If language is Hindi, avoid Latin/English words in output; prefer Devanagari script for all terms.
- if subtopics are provided, structure the script to naturally cover each subtopic
- keep facts generic unless explicitly provided
- no code fences, no explanations outside JSON
`.trim();
}

function normalizeScriptPackage(data, input) {
  return {
    title: safeTrim(data?.title) || safeTrim(input.topic),
    hook: safeTrim(data?.hook),
    summaryLong: safeTrim(data?.summaryLong),
    videoScript: safeTrim(data?.videoScript),
    cta: safeTrim(data?.cta),
    estimatedWords: clampNumber(data?.estimatedWords, 20, 5000, 0),
  };
}

function buildSplitPrompt(input) {
  const script = safeTrim(input.script);
  const infoType = normalizeInfoType(input.extraInfoType);
  const subtopics = normalizeSubtopics(input.subtopics, input.extraInfo || "", infoType);
  const language = resolveModelLanguage(input.language);
  const tone = safeTrim(input.tone) || "professional and engaging";
  const platform = safeTrim(input.platform) || "YouTube";
  const targetDurationSec = clampNumber(input.targetDurationSec, 20, 900, 90);
  const preferredSceneCount = clampNumber(input.sceneCount, 2, 20, 8);

  return `
Split and refine the following script into scene-wise content for short-form video production.

Platform: ${platform}
Language: ${language}
Tone: ${tone}
Target duration (seconds): ${targetDurationSec}
Preferred scene count: ${preferredSceneCount}
Extra info type: ${infoType}
Subtopics to cover:
${subtopics.length ? subtopics.map((s, i) => `${i + 1}. ${s}`).join("\n") : "None"}

Source script:
${script}

Return ONLY valid JSON:
{
  "scenes": [
    {
      "heading": "Scene 1 title",
      "narration": "Refined narration text for this scene",
      "onScreenText": "Short text overlay",
      "voiceStyle": "e.g. energetic news style",
      "durationSec": 10
    }
  ]
}

Rules:
- durationSec should be 4-20 each
- total scene duration should be close to target duration
- narration must be polished and production-ready
- headings, narration, and onScreenText must be in ${language}
- If language is Hindi, avoid Latin/English words in headings, narration and onScreenText; prefer Devanagari script.
- If subtopics are provided, create at least one scene per subtopic.
- Always include an Introduction scene and a Conclusion/Thank You scene.
- no markdown and no explanation outside JSON
`.trim();
}

function normalizeScenes(rawScenes, targetDurationSec, sourceScript, preferredSceneCount, subtopics, language) {
  const input = Array.isArray(rawScenes) ? rawScenes : [];
  let cleaned = input
    .map((scene, idx) => ({
      id: idx + 1,
      heading: safeTrim(scene?.heading) || `Scene ${idx + 1}`,
      narration: safeTrim(scene?.narration),
      onScreenText: safeTrim(scene?.onScreenText),
      voiceStyle: safeTrim(scene?.voiceStyle) || "Clear neutral narration",
      durationSec: clampNumber(scene?.durationSec, 4, 20, 8),
    }))
    .filter((scene) => scene.narration);

  if (cleaned.length < 2) {
    const fallback = buildFallbackScenesFromScript(sourceScript, preferredSceneCount, targetDurationSec)
      .map((scene, idx) => ({
        id: idx + 1,
        heading: safeTrim(scene?.heading) || `Scene ${idx + 1}`,
        narration: safeTrim(scene?.narration),
        onScreenText: safeTrim(scene?.onScreenText),
        voiceStyle: safeTrim(scene?.voiceStyle) || "Clear neutral narration",
        durationSec: clampNumber(scene?.durationSec, 4, 20, 8),
      }))
      .filter((scene) => scene.narration);
    cleaned = fallback.length ? fallback : cleaned;
  }

  if (!cleaned.length) {
    return [];
  }

  cleaned = enforceSubtopicCoverage({
    scenes: cleaned,
    subtopics,
    language,
  })
    .map((scene, idx) => ({
      ...scene,
      id: idx + 1,
      heading: safeTrim(scene?.heading) || `Scene ${idx + 1}`,
      narration: safeTrim(scene?.narration),
      onScreenText: safeTrim(scene?.onScreenText),
      voiceStyle: safeTrim(scene?.voiceStyle) || "Clear neutral narration",
      durationSec: clampNumber(scene?.durationSec, 4, 20, 8),
    }))
    .filter((scene) => scene.narration);

  const goal = clampNumber(targetDurationSec, 20, 900, 90);
  const currentTotal = cleaned.reduce((sum, s) => sum + s.durationSec, 0) || 1;
  const scale = goal / currentTotal;

  return cleaned.map((scene) => ({
    ...scene,
    durationSec: clampNumber(scene.durationSec * scale, 4, 20, scene.durationSec),
  }));
}

async function generateScript(input) {
  const systemPrompt =
    "You are a senior content strategist. Always return strict JSON only.";
  const userPrompt = buildScriptGenerationPrompt(input);
  const { parsed, model } = await askModelForJson({
    model: input.model,
    systemPrompt,
    userPrompt,
  });

  return {
    model,
    ...normalizeScriptPackage(parsed, input),
  };
}

async function splitScript(input) {
  const systemPrompt =
    "You are an expert video script editor and scene planner. Return strict JSON only.";
  const userPrompt = buildSplitPrompt(input);
  const { parsed, model } = await askModelForJson({
    model: input.model,
    systemPrompt,
    userPrompt,
  });

  const scenes = normalizeScenes(
    parsed?.scenes,
    input.targetDurationSec,
    input.script,
    input.sceneCount,
    normalizeSubtopics(input.subtopics, input.extraInfo || "", input.extraInfoType),
    input.language,
  );
  const totalDurationSec = scenes.reduce((sum, scene) => sum + scene.durationSec, 0);

  return {
    model,
    scenes,
    totalDurationSec,
  };
}

function resolveTtsDefaults(language) {
  const lang = safeTrim(language).toLowerCase() || "en";
  if (lang === "hi") return { language: "hi", model: "paratham" };
  return { language: "en", model: "lessac" };
}

async function generateSceneAudios({ scenes, language, model, tuning, options }) {
  const fallback = resolveTtsDefaults(language);
  const selectedLanguage = safeTrim(language).toLowerCase() || fallback.language;
  const selectedModel = safeTrim(model).toLowerCase() || fallback.model;
  const list = Array.isArray(scenes) ? scenes : [];

  const out = [];
  for (let i = 0; i < list.length; i += 1) {
    const scene = list[i] || {};
    const narration = safeTrim(scene?.narration || scene?.text);
    if (!narration) continue;

    const audio = await ttsService.generateAudio({
      text: narration,
      language: selectedLanguage,
      model: selectedModel,
      tuning,
      options,
    });

    out.push({
      id: scene?.id || i + 1,
      heading: safeTrim(scene?.heading) || `Scene ${i + 1}`,
      narration,
      fileName: audio.fileName,
      audioUrl: `user/tts/audio/${audio.fileName}`,
      durationSec: clampNumber(
        Math.max(
          Number(scene?.durationSec || 0),
          Number(audio?.durationSec || 0) + SCENE_AUDIO_PADDING_SEC,
        ),
        1,
        1200,
        0,
      ),
    });
  }

  return {
    language: selectedLanguage,
    model: selectedModel,
    scenes: out,
  };
}

module.exports = {
  generateScript,
  splitScript,
  generateSceneAudios,
};
