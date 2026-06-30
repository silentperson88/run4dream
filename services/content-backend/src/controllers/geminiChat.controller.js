const { chatWithGemini, askGeminiWithGoogleSearch } = require("../services/gemini.service");

const NEWS_ANALYST_SYSTEM_PROMPT = `
You are a concise news-analysis assistant.

Primary job:
- Answer the user's question by analyzing news, market context, business context, or current-event framing.

Response style:
- Keep the answer short and direct.
- Prefer 3 to 6 short lines or bullets.
- Avoid fluff, disclaimers, and repetition.
- Use simple language.

Reasoning style:
- Focus on what happened, why it matters, and the likely takeaway.
- If the user asks about a stock/company/news event, explain the impact clearly.
- If the question is broad, summarize the most important angle first.

If context is missing:
- Say briefly what exact missing detail is needed.
- Do not invent facts.

Output rule:
- Default to a concise answer under 120 words unless the user explicitly asks for more.
`.trim();

const geminiChat = async (req, res) => {
  try {
    const messages = Array.isArray(req.body?.messages) ? req.body.messages : [];
    const model = String(req.body?.model || process.env.GEMINI_MODEL || "gemini-2.5-flash").trim();
    const temperature = req.body?.temperature;

    if (!messages.length) {
      return res.status(400).json({
        success: false,
        message: "messages are required",
      });
    }

    const result = await chatWithGemini({
      messages,
      model,
      temperature,
      systemPrompt: NEWS_ANALYST_SYSTEM_PROMPT,
    });

    return res.status(200).json({
      success: true,
      data: {
        model: result.model,
        response: result.text,
      },
    });
  } catch (error) {
    return res.status(400).json({
      success: false,
      message: error.message || "Failed to chat with Gemini",
    });
  }
};

const GROUNDED_NEWS_PROMPT = `
You are a concise market-news answer assistant with Google Search grounding enabled.

Instructions:
- Answer the question using recent web information.
- Keep the answer short and practical.
- Prefer 4 to 7 bullets.
- Focus on why it matters, not generic explanation.
- If the topic is a stock move, explain likely drivers first.
- If sources disagree, say that briefly.
- Do not add filler.
`.trim();

const geminiGroundedAnswer = async (req, res) => {
  try {
    const question = String(req.body?.question || "").trim();
    if (!question) {
      return res.status(400).json({
        success: false,
        message: "question is required",
      });
    }

    const result = await askGeminiWithGoogleSearch({
      question,
      systemPrompt: GROUNDED_NEWS_PROMPT,
    });

    return res.status(200).json({
      success: true,
      data: {
        model: result.model,
        response: result.text,
        grounding: result.grounding,
      },
    });
  } catch (error) {
    return res.status(400).json({
      success: false,
      message: error.message || "Failed to get grounded Gemini answer",
    });
  }
};

module.exports = {
  geminiChat,
  geminiGroundedAnswer,
};
