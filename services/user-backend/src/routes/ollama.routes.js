const express = require("express");
const router = express.Router();
const controller = require("../controllers/ollama.controller");
const validate = require("../middlewares/validateRequest.middleware");
const { ollamaChatValidator } = require("../validator/ollama.validator");

router.post("/chat", ollamaChatValidator, validate, controller.chat);

module.exports = router;
