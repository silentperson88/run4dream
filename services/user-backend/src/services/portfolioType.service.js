const portfolioTypeRepo = require("../repositories/portfolioTypes.repository");

const listPortfolioTypes = () => portfolioTypeRepo.listActive();

module.exports = {
  listPortfolioTypes,
};
