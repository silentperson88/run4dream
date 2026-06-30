const express = require("express");
const { googleAiScrapeAnswer } = require("../controllers/googleAiScrape.controller");

const router = express.Router();

router.post("/google-ai-scrape-answer", googleAiScrapeAnswer);

module.exports = router;
