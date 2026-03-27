const express = require("express");
const router = express.Router();
const controller = require("../controllers/tts.controller");

router.post("/generate", controller.generateTtsAudio);
router.get("/audio/:fileName", controller.streamGeneratedAudio);

module.exports = router;
