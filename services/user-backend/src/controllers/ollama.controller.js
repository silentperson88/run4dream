const { response } = require("../utils/response.utils");
const { chatWithOllama } = require("../services/ollama.service");

async function chat(req, res) {
  try {
    const data = await chatWithOllama({
      text: req.body?.text,
      prompt: req.body?.prompt,
      model: req.body?.model,
      systemPrompt: req.body?.systemPrompt,
    });

    return response(res, 200, "Ollama response generated", data);
  } catch (err) {
    return response(res, 400, err.message || "Unable to process Ollama request");
  }
}

module.exports = {
  chat,
};
