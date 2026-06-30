const GEMINI_API_BASE =
  process.env.GEMINI_API_BASE_URL || "https://generativelanguage.googleapis.com/v1beta";
const DEFAULT_GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";

function extractGeminiText(payload) {
  const candidates = Array.isArray(payload?.candidates) ? payload.candidates : [];
  for (let i = 0; i < candidates.length; i += 1) {
    const parts = candidates[i]?.content?.parts;
    if (!Array.isArray(parts)) continue;
    const text = parts.map((part) => String(part?.text || "")).join("").trim();
    if (text) return text;
  }
  return "";
}

function extractGroundingMetadata(payload) {
  const candidate = Array.isArray(payload?.candidates) ? payload.candidates[0] : null;
  const metadata = candidate?.groundingMetadata || {};
  const groundingChunks = Array.isArray(metadata?.groundingChunks) ? metadata.groundingChunks : [];

  const sources = groundingChunks
    .map((chunk) => ({
      title: String(chunk?.web?.title || "").trim(),
      uri: String(chunk?.web?.uri || "").trim(),
    }))
    .filter((item) => item.title || item.uri);

  return {
    webSearchQueries: Array.isArray(metadata?.webSearchQueries) ? metadata.webSearchQueries : [],
    sources,
  };
}

async function generateGeminiText({
  prompt,
  model = DEFAULT_GEMINI_MODEL,
  temperature = 0.1,
}) {
  const apiKey = String(process.env.GEMINI_API_KEY || "").trim();
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is not configured");
  }
  const selectedModel = String(model || DEFAULT_GEMINI_MODEL).trim();
  if (!selectedModel) {
    throw new Error("Gemini model is required");
  }

  const url = `${String(GEMINI_API_BASE).replace(/\/+$/, "")}/models/${encodeURIComponent(
    selectedModel,
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: [{ role: "user", parts: [{ text: String(prompt || "") }] }],
      generationConfig: {
        temperature: Number(temperature),
      },
    }),
  });

  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message =
      payload?.error?.message || `Gemini API failed with status ${res.status}`;
    throw new Error(message);
  }

  const text = extractGeminiText(payload);
  if (!text) {
    throw new Error("Gemini returned empty response");
  }

  return {
    model: selectedModel,
    text,
    raw: payload,
  };
}

async function chatWithGemini({
  messages = [],
  model = DEFAULT_GEMINI_MODEL,
  temperature = 0.4,
  systemPrompt = "",
}) {
  const apiKey = String(process.env.GEMINI_API_KEY || "").trim();
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is not configured");
  }

  const selectedModel = String(model || DEFAULT_GEMINI_MODEL).trim();
  if (!selectedModel) {
    throw new Error("Gemini model is required");
  }

  const normalizedMessages = Array.isArray(messages)
    ? messages
        .map((message) => ({
          role: String(message?.role || "").toLowerCase() === "assistant" ? "model" : "user",
          parts: [{ text: String(message?.content || "").trim() }],
        }))
        .filter((message) => message.parts[0].text)
    : [];

  const promptPrefix = String(systemPrompt || "").trim();
  const finalMessages = promptPrefix
    ? [{ role: "user", parts: [{ text: promptPrefix }] }, ...normalizedMessages]
    : normalizedMessages;

  if (!finalMessages.length) {
    throw new Error("At least one message is required");
  }

  const url = `${String(GEMINI_API_BASE).replace(/\/+$/, "")}/models/${encodeURIComponent(
    selectedModel,
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: finalMessages,
      generationConfig: {
        temperature: Number(temperature),
      },
    }),
  });

  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message =
      payload?.error?.message || `Gemini API failed with status ${res.status}`;
    throw new Error(message);
  }

  const text = extractGeminiText(payload);
  if (!text) {
    throw new Error("Gemini returned empty response");
  }

  return {
    model: selectedModel,
    text,
    raw: payload,
  };
}

async function askGeminiWithGoogleSearch({
  question,
  model = process.env.GEMINI_GROUNDED_MODEL || "gemini-2.5-flash-lite",
  temperature = 0.2,
  systemPrompt = "",
}) {
  const apiKey = String(process.env.GEMINI_API_KEY || "").trim();
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is not configured");
  }

  const selectedModel = String(model || "").trim();
  if (!selectedModel) {
    throw new Error("Grounded Gemini model is required");
  }

  const userQuestion = String(question || "").trim();
  if (!userQuestion) {
    throw new Error("Question is required");
  }

  const url = `${String(GEMINI_API_BASE).replace(/\/+$/, "")}/models/${encodeURIComponent(
    selectedModel,
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const promptText = [String(systemPrompt || "").trim(), userQuestion].filter(Boolean).join("\n\n");

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: [{ role: "user", parts: [{ text: promptText }] }],
      tools: [{ google_search: {} }],
      generationConfig: {
        temperature: Number(temperature),
      },
    }),
  });

  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message =
      payload?.error?.message || `Gemini API failed with status ${res.status}`;
    throw new Error(message);
  }

  const text = extractGeminiText(payload);
  if (!text) {
    throw new Error("Gemini returned empty grounded response");
  }

  return {
    model: selectedModel,
    text,
    grounding: extractGroundingMetadata(payload),
    raw: payload,
  };
}

module.exports = {
  generateGeminiText,
  chatWithGemini,
  askGeminiWithGoogleSearch,
};
