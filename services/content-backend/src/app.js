const express = require("express");
const cors = require("cors");
const { BaseRoutes } = require("./routes/index.route");
const { authMiddleware } = require("./middlewares/auth.middleware");
const rssController = require("./controllers/rss.controller");

const app = express();

app.get("/health", (_, res) => res.status(200).json({ ok: true, server: "content service" }));
app.get("/api/v1/content/health", (_, res) => res.status(200).json({ ok: true, server: "content service" }));

app.use(cors());
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));

app.post(
  ["/news-content/rss/improve-template", "/api/v1/content/news-content/rss/improve-template"],
  authMiddleware,
  rssController.improveSocialTemplate,
);
app.get(
  ["/news-content/rss/template-prompts", "/api/v1/content/news-content/rss/template-prompts"],
  authMiddleware,
  rssController.getTemplatePrompts,
);

BaseRoutes(app);

app.use((req, res) => res.status(404).json({ message: "Not found" }));

module.exports = app;
