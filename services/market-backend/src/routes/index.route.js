const baseRoute = "";

module.exports = (app) => {
  app.use(`${baseRoute}/admin`, require("./admin.route"));
  app.use(`${baseRoute}/master`, require("./stockMaster.route"));
  app.use(`${baseRoute}/activestock`, require("./activestock.route"));
  app.use(`${baseRoute}/fundamentals/schema-audit`, require("./fundamentalsSchemaAudit.route"));
  app.use(`${baseRoute}/fundamentals`, require("./stockFundamental.route"));
  app.use(`${baseRoute}/eod`, require("./stockOhlcEod.route"));
  app.use(`${baseRoute}/ipo-gmp`, require("./ipoGmp.route"));
  app.use(`${baseRoute}/redis`, require("./redis.route"));
};
