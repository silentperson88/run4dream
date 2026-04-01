const { body, param } = require("express-validator");

const taxPlannerPlanValidator = [
  body("name")
    .optional()
    .isString()
    .withMessage("name must be a string")
    .isLength({ min: 1, max: 160 })
    .withMessage("name must be between 1 and 160 characters"),
  body("buyLots")
    .optional()
    .isArray({ max: 500 })
    .withMessage("buyLots must be an array with up to 500 items"),
  body("sellTrades")
    .optional()
    .isArray({ max: 500 })
    .withMessage("sellTrades must be an array with up to 500 items"),
  body("settings")
    .optional()
    .isObject()
    .withMessage("settings must be an object"),
];

const taxPlannerPlanIdValidator = [
  param("planId")
    .isInt({ min: 1 })
    .withMessage("planId must be a valid positive number"),
];

module.exports = {
  taxPlannerPlanValidator,
  taxPlannerPlanIdValidator,
};
