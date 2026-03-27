const { body } = require("express-validator");

const ollamaChatValidator = [
  body().custom((value) => {
    const text = String(value?.text || "").trim();
    const prompt = String(value?.prompt || "").trim();
    if (!text && !prompt) {
      throw new Error("Either text or prompt is required");
    }
    return true;
  }),
  body("text")
    .optional()
    .isString()
    .withMessage("text must be a string")
    .isLength({ min: 1, max: 10000 })
    .withMessage("text must be between 1 and 10000 characters"),
  body("prompt")
    .optional()
    .isString()
    .withMessage("prompt must be a string")
    .isLength({ min: 1, max: 10000 })
    .withMessage("prompt must be between 1 and 10000 characters"),
  body("model")
    .optional()
    .isString()
    .withMessage("model must be a string")
    .isLength({ min: 1, max: 100 })
    .withMessage("model must be between 1 and 100 characters"),
  body("systemPrompt")
    .optional()
    .isString()
    .withMessage("systemPrompt must be a string")
    .isLength({ max: 2000 })
    .withMessage("systemPrompt can be up to 2000 characters"),
];

module.exports = {
  ollamaChatValidator,
};
