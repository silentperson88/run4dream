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

module.exports = {
  generateGeminiText,
};

