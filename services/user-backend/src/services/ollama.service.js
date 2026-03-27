const OLLAMA_BASE_URL = process.env.OLLAMA_BASE_URL || "http://127.0.0.1:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "gemma3:1b";
const OLLAMA_TIMEOUT_MS = Number(process.env.OLLAMA_TIMEOUT_MS || 30000);
const OLLAMA_SYSTEM_PROMPT = process.env.OLLAMA_SYSTEM_PROMPT || "";

function normalizeText(value) {
  return String(value || "").trim();
}

async function chatWithOllama({ text, prompt, model, systemPrompt, format, options }) {
  if (typeof fetch !== "function") {
    throw new Error("Fetch API is not available. Use Node.js 18+.");
  }

  const userText = normalizeText(prompt || text);
  if (!userText) {
    throw new Error("Text is required");
  }

  const selectedModel = normalizeText(model) || OLLAMA_MODEL;
  const selectedSystemPrompt = normalizeText(systemPrompt) || OLLAMA_SYSTEM_PROMPT;
  const messages = [];

  if (selectedSystemPrompt) {
    messages.push({ role: "system", content: selectedSystemPrompt });
  }
  messages.push({ role: "user", content: userText });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OLLAMA_TIMEOUT_MS);

  try {
    const res = await fetch(`${OLLAMA_BASE_URL}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: selectedModel,
        stream: false,
        messages,
        ...(format ? { format } : {}),
        ...(options && typeof options === "object" ? { options } : {}),
      }),
      signal: controller.signal,
    });

    const raw = await res.text();
    let payload = null;
    try {
      payload = raw ? JSON.parse(raw) : {};
    } catch (_) {
      payload = { raw };
    }

    if (!res.ok) {
      const errMsg =
        payload?.error || payload?.message || `Ollama request failed with status ${res.status}`;
      throw new Error(errMsg);
    }

    const answer = payload?.message?.content || "";
    return {
      model: payload?.model || selectedModel,
      response: answer,
      done: payload?.done === true,
      created_at: payload?.created_at || null,
      usage: {
        prompt_eval_count: payload?.prompt_eval_count ?? null,
        eval_count: payload?.eval_count ?? null,
      },
    };
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(`Ollama request timed out after ${OLLAMA_TIMEOUT_MS}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

module.exports = {
  chatWithOllama,
};
