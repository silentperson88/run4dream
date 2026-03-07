module.exports.responseUtils = {
  /* common */
  ERROR: "Internal server error",
  NOT_ALLOWED_TO_LOGIN_FROM_MOBILE: "User account is not registered",
  NOT_ALLOWED_TO_LOGIN_FROM_ADMINPANEL: "Client account is not registered",
  SOMETHING_WENT_WRONG: "Something went wrong",
  SUCCESS: "Success",
  NO_DATA_FOUND: "No data found",
  UNAUTHORIZED_ACCESS: "Unauthorized access",
  SERVER_ERROR: "Server error",

  //   Admin
  SERVER_NOT_LOGIN: "Server is not logged in today",
  SERVER_OFFLINE: "Server is offline",
  SERVER_ONLINE: "Server is online",

  // Raw stock
  RAW_STOCK_NOT_FOUND: "Raw stock not found",
  INVALID_STATUS_VALUE: "Invalid status value",
  RAW_STOCK_ID_AND_STATUS_REQUIRED: "Raw stock id and status is required",
  RAW_STOCK_REJECTED_SUCCESSFULLY: "Raw stock rejected successfully",

  // Master
  STOCK_ALREADY_EXISTS: "Stock already exists",
  ACTIVE_STOCK_NOT_FOUND: "Active stock not found",
  STOCK_NOT_FOUND: "Stock not found",
  MASTER_CRATED_SUCCESSFULLY: "Master stock created successfully",
  MASTER_FETCHED_SUCCESSFULLY: "Master list fetched successfully",
  SCREENER_URL_NOT_EXIST: "Screener url not exist",
  MASTER_ID_REQUIRED: "Master stock id is required",

  //   Stock Fundamental
  FAILED_TO_FETCH_STOCK_FUNDAMENTALS: "Failed to fetch stock fundamentals",
  FAILED_TO_UPDATE_STOCK_FUNDAMENTALS: "Failed to update stock fundamentals",
};
