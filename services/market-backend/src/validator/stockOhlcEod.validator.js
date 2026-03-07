const { body, param, query } = require("express-validator");

exports.fetchEodByRangeValidationRule = [
  body("master_id")
    .notEmpty()
    .withMessage("master_id is required")
    .isInt({ min: 1 })
    .withMessage("master_id must be a valid id"),
  body("fromDate")
    .notEmpty()
    .withMessage("fromDate is required")
    .isISO8601({ strict: true, strictSeparator: true })
    .withMessage("fromDate must be in YYYY-MM-DD format"),
  body("toDate")
    .notEmpty()
    .withMessage("toDate is required")
    .isISO8601({ strict: true, strictSeparator: true })
    .withMessage("toDate must be in YYYY-MM-DD format")
    .custom((toDate, { req }) => {
      const { fromDate } = req.body;

      if (!fromDate) return true;

      const from = new Date(`${fromDate}T00:00:00.000Z`);
      const to = new Date(`${toDate}T00:00:00.000Z`);

      if (from > to) {
        throw new Error("fromDate must be less than or equal to toDate");
      }

      return true;
    }),
];

exports.getEodFromDbValidationRule = [
  param("master_id")
    .notEmpty()
    .withMessage("master_id is required")
    .isInt({ min: 1 })
    .withMessage("master_id must be a valid id"),
  query("fromDate")
    .optional()
    .isISO8601({ strict: true, strictSeparator: true })
    .withMessage("fromDate must be in YYYY-MM-DD format"),
  query("toDate")
    .optional()
    .isISO8601({ strict: true, strictSeparator: true })
    .withMessage("toDate must be in YYYY-MM-DD format")
    .custom((toDate, { req }) => {
      const { fromDate } = req.query;
      if (!fromDate || !toDate) return true;

      const from = new Date(`${fromDate}T00:00:00.000Z`);
      const to = new Date(`${toDate}T00:00:00.000Z`);
      if (from > to) {
        throw new Error("fromDate must be less than or equal to toDate");
      }
      return true;
    }),
  query("limit")
    .optional()
    .isInt({ min: 1, max: 20000 })
    .withMessage("limit must be between 1 and 20000"),
];
