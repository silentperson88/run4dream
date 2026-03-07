const { query } = require("express-validator");

exports.dashboardQueryValidator = [
  query("days")
    .optional()
    .isInt({ min: 1, max: 365 })
    .withMessage("days must be between 1 and 365"),
];
