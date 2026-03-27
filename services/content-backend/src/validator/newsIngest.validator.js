const { body } = require("express-validator");

const fetchAnnouncementsValidator = [
  body("date")
    .optional()
    .isISO8601({ strict: true, strictSeparator: true })
    .withMessage("date must be in YYYY-MM-DD format"),
  body("category").optional().isString().isLength({ max: 20 }),
  body("scrip").optional().isString().isLength({ max: 40 }),
  body("search").optional().isString().isLength({ max: 40 }),
  body("annType").optional().isString().isLength({ max: 10 }),
  body("subcategory").optional().isString().isLength({ max: 40 }),
  body("minScore")
    .optional()
    .isInt({ min: 0, max: 100 })
    .withMessage("minScore must be between 0 and 100"),
  body("maxPages")
    .optional()
    .isInt({ min: 0, max: 100 })
    .withMessage("maxPages must be between 0 and 100"),
  body("timeoutMs")
    .optional()
    .isInt({ min: 5000, max: 120000 })
    .withMessage("timeoutMs must be between 5000 and 120000"),
  body("extraKeywords")
    .optional()
    .isString()
    .isLength({ max: 1000 })
    .withMessage("extraKeywords must be up to 1000 chars"),
  body("limit")
    .optional()
    .isInt({ min: 1, max: 200 })
    .withMessage("limit must be between 1 and 200"),
  body("offset")
    .optional()
    .isInt({ min: 0, max: 100000 })
    .withMessage("offset must be between 0 and 100000"),
];

module.exports = {
  fetchAnnouncementsValidator,
};
