const express = require("express");
const router = express.Router();

const {
  fetchLiveIpoGmp,
  getLiveIpoGmpPaginated,
} = require("../controllers/ipoGmp.controller");

router.get("/", getLiveIpoGmpPaginated);

router.post("/fetch", fetchLiveIpoGmp);

module.exports = router;
