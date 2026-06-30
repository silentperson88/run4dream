const express = require("express");
const router = express.Router();

const { getTopics } = require("../controllers/stockShorts.controller");

router.get("/topics", getTopics);

module.exports = router;
