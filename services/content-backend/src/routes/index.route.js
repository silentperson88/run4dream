const baseRoute = "";

const BaseRoutes = (app) => {
  app.use(`${baseRoute}/news-content`, require("./newsCreator.routes"));
  app.use(`${baseRoute}/news-content`, require("./newsContentVideos.routes"));
  app.use(`${baseRoute}/news-content`, require("./audioTools.routes"));
  app.use(`${baseRoute}/news-content`, require("./imageSearch.routes"));
  app.use(`${baseRoute}/news-content`, require("./rss.routes"));
  app.use(`${baseRoute}/news-content/new-approach`, require("./newsApproach.routes"));
  app.use(`${baseRoute}/news-content/bse`, require("./newsIngest.routes"));
  app.use(`${baseRoute}/tts`, require("./tts.routes"));
};

module.exports = { BaseRoutes };
