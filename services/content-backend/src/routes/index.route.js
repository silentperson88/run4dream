const baseRoute = "";

const mountRoutes = (app, prefix = "") => {
  app.use(`${prefix}/news-content`, require("./newsCreator.routes"));
  app.use(`${prefix}/news-content`, require("./newsContentVideos.routes"));
  app.use(`${prefix}/news-content`, require("./audioTools.routes"));
  app.use(`${prefix}/news-content`, require("./imageSearch.routes"));
  app.use(`${prefix}/news-content`, require("./rss.routes"));
  app.use(`${prefix}/news-content`, require("./musicLibrary.routes"));
  app.use(`${prefix}/news-content`, require("./socialAccounts.routes"));
  app.use(`${prefix}/news-content`, require("./socialPublish.routes"));
  app.use(`${prefix}/news-content/new-approach`, require("./newsApproach.routes"));
  app.use(`${prefix}/news-content/bse`, require("./newsIngest.routes"));
  app.use(`${prefix}/tts`, require("./tts.routes"));
};

const BaseRoutes = (app) => {
  mountRoutes(app, baseRoute);
  // Keep the API reachable both directly and through the gateway rewrite.
  mountRoutes(app, "/api/v1/content");
};

module.exports = { BaseRoutes };
