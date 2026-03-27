const baseRoute = "";

const BaseRoutes = (app) => {
  app.use(`${baseRoute}/auth`, require("./auth.routes"));
  app.use(`${baseRoute}/portfolio-types`, require("./portfolioType.route"));
  app.use(`${baseRoute}/my-portfolios`, require("./userPortfolio.routes"));
  app.use(`${baseRoute}/wallet`, require("./userWallet.routes"));
  app.use(`${baseRoute}/dashboard`, require("./dashboard.routes"));
  app.use(`${baseRoute}/order`, require("./order.route"));
  app.use(`${baseRoute}/tts`, require("./tts.routes"));
  app.use(`${baseRoute}/ollama`, require("./ollama.routes"));
  app.use(`${baseRoute}/content`, require("./contentCreator.routes"));
};

module.exports = { BaseRoutes };
