require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { startMarketScheduler } = require("./schedulers/market.scheduler");
const { startFundamentalsScheduler } = require("./schedulers/fundamentals.scheduler");
const { startIpoGmpScheduler } = require("./schedulers/ipoGmp.scheduler");

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use(
  cors({
    origin: "*",
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE",
  }),
);

// routes
const baseRoute = "/";
app.get("/health", (_, res) => res.json({ ok: true, server: "Price Tracker" }));
require("./routes/index.route")(app, baseRoute);

// Start market scheduler
async function bootstrap() {
  console.log("🚀 Backend starting...");
  // await startMarketScheduler();
  const role = (process.env.FUNDAMENTALS_SCHEDULER_ROLE || "api").toLowerCase();
  // if (role === "api" || role === "both") {
  //   startFundamentalsScheduler();
  // }
  if ((process.env.IPO_GMP_SCHEDULER_ENABLED || "true").toLowerCase() === "true") {
    startIpoGmpScheduler();
  }
}

bootstrap();

module.exports = app;
