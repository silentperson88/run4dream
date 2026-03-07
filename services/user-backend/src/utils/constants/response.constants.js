const MESSAGES = {
  success: true,
  error: false,

  AUTH: {
    UNAUTHORIZED: "Unauthorized access",
    TOKEN_MISSING: "Authentication token missing",
    INVALID_TOKEN: "Invalid authentication token",
    USER_INACTIVE: "User is inactive",
    TOKEN_EXPIRED: "Authentication token expired",
  },

  PORTFOLIO: {
    FAILED_TO_FETCHED_TYPE: "Failed to fetch portfolio type",
    CREATED: "Portfolio created successfully",
    TYPE_NOT_FOUND: "Invalid portfolio type",
    TYPE_REQUIRED: "Portfolio type is required",
    NAME_REQUIRED: "Portfolio name is required",
    DISCLAIMER_REQUIRED: "You must accept all disclaimers",
    INVALID_FUND: "Initial fund must be a valid number",
    INITIAL_FUND_INVALID: "Initial fund must be a valid number",
    CREATE_FAILED: "Unable to create portfolio",
    ALREADY_EXISTS: "Portfolio already exists with same name",
  },

  ORDER: {
    ORDER_PLACED: "Order placed successfully",
    ORDER_FAILED: "Unable to place order",
    INVALID_STOCK: "Invalid or inactive stock",
    INSUFFICIENT_FUNDS: "Insufficient available funds",
    INSUFFICIENT_QTY: "Insufficient quantity to sell",
    ORDER_REJECTED: "Order rejected due to validation failure",
    ORDER_REJECTED_OUT_OF_CIRCUIT: "Order rejected(Out of circuit)",

    FULL_DIFF_PERCENT: 0.1,
    PARTIAL_MIN_PERCENT: 0.11,
    PARTIAL_MAX_PERCENT: 0.2,
    PARTIAL_MIN_QTY_RATIO: 0.2,
    PARTIAL_MAX_QTY_RATIO: 0.6,
  },

  COMMON: {
    VALIDATION_ERROR: "Validation error",
    SOMETHING_WENT_WRONG: "Something went wrong",
    SUCCESS: "Success",
    CACHE_ERROR: "Cache error",
  },
};

module.exports = { MESSAGES };
