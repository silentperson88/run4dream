const express = require("express");
const cors = require("cors");
const { BaseRoutes } = require("./routes/index.route");

const app = express();

app.get("/health", (_, res) => res.status(200).json({ ok: true, server: "content service" }));
app.get("/api/v1/content/health", (_, res) => res.status(200).json({ ok: true, server: "content service" }));

app.use(cors());
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

BaseRoutes(app);

app.use((req, res) => res.status(404).json({ message: "Not found" }));

module.exports = app;
