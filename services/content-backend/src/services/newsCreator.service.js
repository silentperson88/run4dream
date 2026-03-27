const { chatWithOllama } = require("./ollama.service");
const ttsService = require("./tts.service");
const { generateGeminiText } = require("./gemini.service");

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

function normalizeParagraphScript(value) {
  const raw = String(value || "");
  const noMarkdown = raw.replace(/\*\*/g, " ");
  const noTimestamps = noMarkdown.replace(/\(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}\)/g, " ");
  const noLabels = noTimestamps
    .replace(/\b(visuals?|voiceover|opening|call to action|end screen)\s*:/gi, " ")
    .replace(/\b(end of presentation)\b/gi, " ");
  return noLabels.replace(/\s+/g, " ").trim();
}

function hasLatinChars(text) {
  return /[A-Za-z]/.test(String(text || ""));
}

function extractLatinTokens(text) {
  const matches = String(text || "").match(/\b[A-Za-z][A-Za-z0-9_.+\-]*\b/g);
  return Array.isArray(matches) ? Array.from(new Set(matches)) : [];
}

function escapeRegex(text) {
  return String(text || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isHindiLanguage(language) {
  const raw = safeTrim(language).toLowerCase();
  return raw === "hi" || raw === "hindi";
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

async function rewriteToPureHindiDevanagari(text, model) {
  const input = safeTrim(text);
  if (!input) return "";
  let working = input;

  for (let attempt = 0; attempt < 4; attempt += 1) {
    if (!hasLatinChars(working)) break;
    const latinTokens = extractLatinTokens(working).slice(0, 80);
    const response = await chatWithOllama({
      model,
      systemPrompt:
        "आप हिंदी स्क्रिप्ट सुधार विशेषज्ञ हैं। आउटपुट में केवल देवनागरी लिखें, कोई Latin/English अक्षर नहीं।",
      text: `
नीचे दिए गए टेक्स्ट को केवल देवनागरी हिंदी में दोबारा लिखो।

नियम:
- आउटपुट में A-Z या a-z का एक भी अक्षर नहीं होना चाहिए।
- कंपनी नाम, व्यक्ति नाम, अंग्रेज़ी शब्द/संक्षेप (जैसे GTFL, QIP) को हिंदी उच्चारण के अनुसार देवनागरी में लिखो।
- अर्थ वही रखो, अनावश्यक बदलाव मत करो।
- केवल अंतिम टेक्स्ट दो, कोई व्याख्या नहीं।
${latinTokens.length ? `- इन रोमन शब्दों को देवनागरी में अवश्य बदलो: ${latinTokens.join(", ")}` : ""}

टेक्स्ट:
${working}
`.trim(),
      options: {
        temperature: 0,
      },
    });
    working = safeTrim(response?.response || working);
  }

  return working;
}

async function convertSentenceToHindiNoLatin(sentence, model, topic) {
  const input = safeTrim(sentence);
  if (!input) return "";
  if (!hasLatinChars(input)) return input;

  const response = await chatWithOllama({
    model,
    systemPrompt:
      "आप हिंदी न्यूज़ वॉइसओवर विशेषज्ञ हैं। आउटपुट में केवल देवनागरी लिखें, रोमन अक्षर बिल्कुल नहीं।",
    text: `
नीचे दिए वाक्य को हिंदी देवनागरी में लिखो।
यदि कोई शब्द अनुवादित नहीं हो सकता तो उसका हिंदी उच्चारण देवनागरी में लिखो।
एक भी A-Z / a-z अक्षर नहीं होना चाहिए।
केवल अंतिम वाक्य दो।
${topic ? `विषय: ${topic}` : ""}

वाक्य:
${input}
`.trim(),
    options: { temperature: 0 },
  });

  return safeTrim(response?.response || input);
}

async function transliterateLatinTokenNoLatin(token, model) {
  const input = safeTrim(token);
  if (!input) return input;
  const response = await chatWithOllama({
    model,
    systemPrompt:
      "You convert English tokens into Hindi Devanagari pronunciation. Output only one Devanagari token.",
    text: `
Convert this token to Hindi Devanagari pronunciation.
Rules:
- Output only Devanagari.
- No English letters.
- No explanation.

Token: ${input}
`.trim(),
    options: { temperature: 0 },
  });
  return safeTrim(response?.response || "");
}

async function enforceNoLatinByTokenReplacement(text, model) {
  let working = safeTrim(text);
  if (!working) return working;
  for (let pass = 0; pass < 3; pass += 1) {
    const tokens = extractLatinTokens(working).slice(0, 120);
    if (!tokens.length) break;
    for (let i = 0; i < tokens.length; i += 1) {
      const token = tokens[i];
      const mapped = await transliterateLatinTokenNoLatin(token, model);
      if (!mapped || hasLatinChars(mapped)) continue;
      const re = new RegExp(`\\b${escapeRegex(token)}\\b`, "g");
      working = working.replace(re, mapped);
    }
  }
  return working;
}

function buildScriptGenerationPrompt(input) {
  const topic = safeTrim(input.topic);
  const requestedLength = String(input.scriptLength || "short").trim().toLowerCase() === "long" ? "long" : "short";
  const rawContext = safeTrim(input.context || input.extraInfo);
  const context = requestedLength === "short" ? rawContext.slice(0, 1800) : rawContext.slice(0, 5000);
  const infoType = normalizeInfoType(input.extraInfoType);
  const subtopics = normalizeSubtopics(input.subtopics, context, infoType);
  const tone = safeTrim(input.tone) || "professional and engaging";
  const platform = safeTrim(input.platform) || "YouTube";
  const language = resolveModelLanguage(input.language);
  const targetDurationSec = requestedLength === "short"
    ? clampNumber(input.targetDurationSec, 15, 120, 45)
    : clampNumber(input.targetDurationSec, 20, 900, 90);
  let targetWords = requestedLength === "short"
    ? clampNumber((targetDurationSec * 1.8).toFixed(0), 70, 260, 120)
    : clampNumber((targetDurationSec * 2.2).toFixed(0), 60, 1800, 220);
  const summaryWordsMin = requestedLength === "short"
    ? clampNumber(Math.round(targetDurationSec * 1.3), 40, 180, 80)
    : clampNumber(Math.round(targetDurationSec * 2.8), 220, 1600, 300);
  const summaryWordsMax = requestedLength === "short"
    ? clampNumber(Math.round(targetDurationSec * 1.9), 70, 260, 120)
    : clampNumber(Math.round(targetDurationSec * 4.2), 320, 2400, 600);
  let scriptWordsMin = clampNumber(input.scriptWordsMin, 30, 8000, 0);
  let scriptWordsMax = clampNumber(input.scriptWordsMax, 30, 8000, 0);

  if (scriptWordsMin || scriptWordsMax) {
    if (!scriptWordsMin) scriptWordsMin = Math.max(30, Math.round(targetWords * 0.7));
    if (!scriptWordsMax) scriptWordsMax = Math.max(scriptWordsMin, Math.round(targetWords * 1.3));
    if (scriptWordsMax < scriptWordsMin) scriptWordsMax = scriptWordsMin;
    targetWords = clampNumber(Math.round((scriptWordsMin + scriptWordsMax) / 2), 30, 8000, targetWords);
  }
  const scriptWordsRule = scriptWordsMin || scriptWordsMax
    ? `- videoScript: ${scriptWordsMin}-${scriptWordsMax} words, ${requestedLength === "short" ? "short news script" : "one continuous paragraph narration"}, no timestamps, no scene headings, no bullet points, no markdown`
    : `- videoScript: ${requestedLength === "short" ? "short news script" : "one continuous paragraph narration"}, no timestamps, no scene headings, no bullet points, no markdown`;

  if (isHindiLanguage(language)) {
    return `
तुम "रन फॉर ड्रीम न्यूज़" YouTube चैनल के senior हिंदी script writer हो। तुम्हारे पास 10 साल का news presenting experience है। तुम्हारी writing style Ravish Kumar जैसी गहरी और Sudhir Chaudhary जैसी dramatic है — लेकिन tone हमेशा एक दोस्त जैसी रहती है।

तुम्हारे लिखे scripts से लाखों लोग जुड़ते हैं क्योंकि तुम हर खबर को आम आदमी की ज़िंदगी से जोड़ते हो।

विषय: ${topic}
खबर का संदर्भ:
${context || "उपलब्ध नहीं"}

STRICT OUTPUT RULES:
- पूरी script सिर्फ हिंदी में — एक भी अंग्रेज़ी शब्द नहीं
- जो शब्द translate नहीं होते उन्हें हिंदी उच्चारण में लिखो जैसे "डाईमेथाइल ईथर", "गूगल", "यूट्यूब"
- कोई bracket नहीं, कोई label नहीं, कोई section heading नहीं
- सिर्फ बोलने वाला text — जो directly audio में जा सके
- passive voice बिल्कुल नहीं — हमेशा active voice
- "..." का use करो dramatic pause के लिए
- script बोलने में 6 से 8 मिनट की होनी चाहिए

SCRIPT STRUCTURE — इसी क्रम में लिखो:

HOOK — 3 से 4 चौंकाने वाले वाक्य जो viewer को पहले 5 सेकंड में रोक दें। कोई अभिवादन नहीं। सीधे सबसे shocking बात से शुरू करो। सवाल पूछो जो दर्शक के दिल को छू जाए।

GREETING — सिर्फ एक वाक्य। "नमस्कार दोस्तों, आप देख रहे हैं रन फॉर ड्रीम न्यूज़।"

SUMMARY — 4 से 5 वाक्य में खबर का सार। क्या हुआ, कहाँ हुआ, किसने किया।

DEPTH — यह सबसे लंबा हिस्सा है। इसमें यह सब होना चाहिए —
पहले background बताओ कि यह क्यों हुआ
फिर हर बड़े fact को देसी उदाहरण से समझाओ जैसे चाय की दुकान, सब्ज़ी मंडी, ऑटो, किराने की दुकान
फिर आम आदमी पर असर बताओ
बीच-बीच में यह phrases use करो — "और यहीं पर मोड़ है", "रुकिए अभी और बाकी है", "सोचिए ज़रा", "यह बात ध्यान से सुनिए", "और यहाँ twist है"
एक key fact को repeat करो जैसे "करोड़ों लोग। करोड़ों।"
कम से कम एक जगह emotional acknowledgment दो जैसे "हम जानते हैं यह सुनकर मन भारी होता है"

OPINION — दर्शकों से एक direct सवाल पूछो। फिर naturally सदस्यता के लिए कहो — जबरदस्ती नहीं, दोस्त की तरह।

OUTRO — एक line में अगले वीडियो का hint। फिर विदाई।

TONE RULES — यह सबसे ज़रूरी है:
- दोस्त की तरह बात करो, समाचार वाचक की तरह नहीं
- "यार", "देखिए", "सोचिए ज़रा", "है ना", "बताइए ज़रा" use करो
- हर लंबे वाक्य के बाद एक छोटा punch line दो
- uniform sentence length कभी नहीं — variety रखो
- numbers को हमेशा example से समझाओ जैसे "यानी आपकी जेब से हर महीने 500 रुपये ज़्यादा जाएंगे"
- rhetorical questions बीच-बीच में डालो

SHORT VIDEO — मुख्य script के बाद एक line छोड़कर 30 से 40 सेकंड की short script लिखो जिसमें सिर्फ तीन चीज़ें हों — सबसे shocking line, एक line context जो पूरी बात नहीं बताए, फिर "पूरी कहानी रन फॉर ड्रीम न्यूज़ के ऊपर वाले वीडियो में है — अभी देखिए।"

केवल वैध JSON लौटाओ:
{
  "title": "string",
  "hook": "string",
  "summaryLong": "string",
  "videoScript": "string",
  "cta": "string",
  "estimatedWords": 0
}

नियम:
- JSON के बाहर एक भी शब्द नहीं।
- videoScript में ऊपर दिया पूरा structure शामिल होना चाहिए।
`.trim();
  }

  return `
Generate a ${requestedLength === "short" ? "short, fast-to-speak" : "long-form"} creator script package for this topic.

Topic: ${topic}
News article / context:
${context || "N/A"}
Extra info type: ${infoType}
Platform: ${platform}
Language: ${language}
Tone: ${tone}
Target duration (seconds): ${targetDurationSec}
Target word count approx: ${targetWords}
Subtopics to include (if provided):
${subtopics.length ? subtopics.map((s, i) => `${i + 1}. ${s}`).join("\n") : "None"}

Structure (no headings, just flow in this order):
Hook
News introduction
Key details
Why this news matters
Future impact
Ending question

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
- summaryLong: ${summaryWordsMin}-${summaryWordsMax} words, key-point focused, avoid unnecessary background detail
${scriptWordsRule}
- Do NOT copy sentences verbatim from the input context.
- Rewrite and simplify the information with a conversational, voice-narration tone.
- Use a YouTube news-presenter style in third person.
- Do NOT write as company spokesperson ("we/our/us").
- Present as: "यह खबर है...", "कंपनी ने बताया...", "रिपोर्ट के अनुसार..." where appropriate.
- write all fields in ${language} language
- If language is Hindi:
  - Output strictly in Devanagari only.
  - Do not use Latin/English script at all.
  - Convert company names, people names, and technical English terms into Hindi phonetic writing (for example, "Reliance" -> "रिलायंस", "QIP" -> "क्यूआईपी").
  - Keep tone natural for Hindi voice narration and pronunciation.
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
    videoScript: normalizeParagraphScript(data?.videoScript),
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
- narration tone must be news anchor / reporter style, third person
- do not use first-person company voice ("we/our/us")
- headings, narration, and onScreenText must be in ${language}
- If language is Hindi:
  - Use only Devanagari script.
  - Convert company names and English terms into Hindi phonetic writing for clear TTS pronunciation.
  - Keep each scene concise and focused on main points only.
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

  const normalized = normalizeScriptPackage(parsed, input);
  if (isHindiLanguage(input.language)) {
    normalized.title = await rewriteToPureHindiDevanagari(normalized.title, input.model);
    normalized.hook = await rewriteToPureHindiDevanagari(normalized.hook, input.model);
    normalized.summaryLong = await rewriteToPureHindiDevanagari(normalized.summaryLong, input.model);
    normalized.videoScript = await rewriteToPureHindiDevanagari(normalized.videoScript, input.model);
    normalized.cta = await rewriteToPureHindiDevanagari(normalized.cta, input.model);
  }

  return {
    model,
    ...normalized,
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

  let scenes = normalizeScenes(
    parsed?.scenes,
    input.targetDurationSec,
    input.script,
    input.sceneCount,
    normalizeSubtopics(input.subtopics, input.extraInfo || "", input.extraInfoType),
    input.language,
  );

  if (isHindiLanguage(input.language)) {
    const rewritten = [];
    for (let i = 0; i < scenes.length; i += 1) {
      const scene = scenes[i];
      rewritten.push({
        ...scene,
        heading: await rewriteToPureHindiDevanagari(scene.heading, input.model),
        narration: await rewriteToPureHindiDevanagari(scene.narration, input.model),
        onScreenText: await rewriteToPureHindiDevanagari(scene.onScreenText, input.model),
      });
    }
    scenes = rewritten;
  }

  const totalDurationSec = scenes.reduce((sum, scene) => sum + scene.durationSec, 0);

  return {
    model,
    scenes,
    totalDurationSec,
  };
}

async function shortenScript(input) {
  const language = resolveModelLanguage(input.language);
  const targetDurationSec = clampNumber(input.targetDurationSec, 15, 300, 45);
  const shortWordsMin = clampNumber(Math.round(targetDurationSec * 2.2), 60, 700, 120);
  const shortWordsMax = clampNumber(Math.round(targetDurationSec * 3.2), 80, 900, 180);
  const original = safeTrim(input.script);

  const prompt = `
Rewrite this script into a shorter version.

Language: ${language}
Target words: ${shortWordsMin}-${shortWordsMax}

Rules:
- Keep only main news points.
- One continuous paragraph only.
- No timestamps, headings, bullets, markdown.
- Keep news presenter tone, third person.
- Do not use company self voice ("we/our/us").
- Preserve key facts and numbers.

Return ONLY valid JSON:
{
  "shortScript": "string"
}

Source script:
${original}
`.trim();

  const { parsed, model } = await askModelForJson({
    model: input.model,
    systemPrompt: "You are a professional script editor. Return strict JSON only.",
    userPrompt: prompt,
  });

  let shortScript = normalizeParagraphScript(parsed?.shortScript || "");
  if (isHindiLanguage(input.language)) {
    shortScript = await rewriteToPureHindiDevanagari(shortScript, input.model);
  }

  return {
    model,
    shortScript,
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

async function convertScriptToHindi(input) {
  const sourceScript = safeTrim(input?.script);
  if (!sourceScript) {
    throw new Error("script is required");
  }
  const model = safeTrim(input?.model) || "gemma3:1b";
  const topic = safeTrim(input?.topic);

  const translatePrompt = `
Rewrite the following script into a YouTube long-video style Hindi news narration (Devanagari).

Strict rules:
- Output must be fully in Devanagari.
- No Latin characters A-Z or a-z.
- If a word cannot be translated (brand, name, technical term), write Hindi phonetic transliteration in Devanagari.
- Keep facts and order unchanged.
- Return only the final script text.

${topic ? `Topic: ${topic}` : ""}

Source script:
${sourceScript}
`.trim();

  const translated = await chatWithOllama({
    model,
    systemPrompt: "You are an expert Hindi news script writer. Output only Devanagari text.",
    text: translatePrompt,
    options: { temperature: 0.1 },
  });

  let hindiScript = safeTrim(translated?.response || "");
  if (!hindiScript) {
    throw new Error("Model did not return hindi script");
  }

  hindiScript = await rewriteToPureHindiDevanagari(hindiScript, model);
  if (hasLatinChars(hindiScript)) {
    const sentences = splitTextBySentences(hindiScript).slice(0, 300);
    if (sentences.length) {
      const rewritten = [];
      for (let i = 0; i < sentences.length; i += 1) {
        const sentenceHindi = await convertSentenceToHindiNoLatin(sentences[i], model, topic);
        rewritten.push(sentenceHindi);
      }
      hindiScript = rewritten.join(" ").replace(/\s+/g, " ").trim();
      hindiScript = await rewriteToPureHindiDevanagari(hindiScript, model);
    }
  }
  if (hasLatinChars(hindiScript)) {
    hindiScript = await enforceNoLatinByTokenReplacement(hindiScript, model);
    hindiScript = await rewriteToPureHindiDevanagari(hindiScript, model);
  }

  if (hasLatinChars(hindiScript)) {
    const latinWords = extractLatinTokens(hindiScript).slice(0, 40).join(", ");
    throw new Error(
      `Hindi conversion incomplete. Roman words still present: ${latinWords || "unknown"}. Please retry.`,
    );
  }

  return {
    model: translated?.model || model,
    hindiScript,
  };
}

async function convertScriptToHindiGemini(input) {
  const sourceScript = safeTrim(input?.script);
  if (!sourceScript) {
    throw new Error("script is required");
  }
  const topic = safeTrim(input?.topic);
  const model = safeTrim(input?.model) || undefined;

  const prompt = `
Convert the following English news into an engaging Hindi YouTube script (3–4 minutes).

Requirements:
- Start with a strong hook (question/shock/curiosity)
- Explain news in simple, conversational Hindi
- Use storytelling style (like a human anchor speaking)
- Add emotional words like: "सोचिए", "अचानक", "अब सवाल यह है"
- Cover: what happened, why, what next, and its impact
- Add a strong conclusion + viewer question + CTA (like, comment, subscribe)

TTS Optimization (IMPORTANT):
- Use short sentences
- Add pauses using "..."
- Break lines frequently
- Avoid long paragraphs
- Make it sound natural and human (not robotic)

Rules:
- No English in output
- No headings
- No explanations
- Keep flow smooth and engaging

${topic ? `Topic: ${topic}` : ""}
Now convert this news:
${sourceScript}
`.trim();

  const result = await generateGeminiText({
    prompt,
    model,
    temperature: 0.1,
  });

  const hindiScript = safeTrim(result?.text || "");
  if (!hindiScript) {
    throw new Error("Gemini did not return hindi script");
  }
  if (hasLatinChars(hindiScript)) {
    const err = new Error(
      "Gemini output still contains English letters. Please retry once.",
    );
    err.partialScript = hindiScript;
    throw err;
  }

  return {
    model: result.model,
    hindiScript,
  };
}

module.exports = {
  generateScript,
  convertScriptToHindi,
  convertScriptToHindiGemini,
  shortenScript,
  splitScript,
  generateSceneAudios,
};
