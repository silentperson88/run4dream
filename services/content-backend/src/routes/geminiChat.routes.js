const express = require("express");
const { geminiChat, geminiGroundedAnswer } = require("../controllers/geminiChat.controller");

const router = express.Router();

router.post("/gemini-chat", geminiChat);
router.post("/gemini-grounded-answer", geminiGroundedAnswer);

module.exports = router;
