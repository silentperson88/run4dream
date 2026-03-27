const { query } = require("express-validator");

const imageSearchValidator = [
  query("query").isString().isLength({ min: 2, max: 200 }).withMessage("query is required"),
  query("provider")
    .optional()
    .isIn(["all", "unsplash", "pexels", "pixabay"])
    .withMessage("provider invalid"),
  query("perPage").optional().isInt({ min: 1, max: 30 }).withMessage("perPage must be 1-30"),
];

module.exports = { imageSearchValidator };
