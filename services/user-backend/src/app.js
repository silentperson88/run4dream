const express = require("express");
const cors = require("cors");
const { BaseRoutes } = require("./routes/index.route");

const app = express();

app.use(cors());
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

BaseRoutes(app);

app.get("/health", (_, res) => res.json({ ok: true, server: "user Service" }));

// run open, partially filled orders in cron job
require("./cron/orderExecution.cron");

module.exports = app;
