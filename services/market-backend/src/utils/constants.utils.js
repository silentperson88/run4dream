module.exports = {
  /* common */
  ERROR: "Internal server error",
  NOT_ALLOWED_TO_LOGIN_FROM_MOBILE: "User account is not registered",
  NOT_ALLOWED_TO_LOGIN_FROM_ADMINPANEL: "Client account is not registered",
  SOMETHING_WENT_WRONG: "Something went wrong",
  SUCCESS: "Success",
  NO_DATA_FOUND: "No data found",
  UNAUTHORIZED_ACCESS: "Unauthorized access",

  //   Admin
  SERVER_NOT_LOGIN: "Server is not logged in today",
  SERVER_OFFLINE: "Server is offline",
  SERVER_ONLINE: "Server is online",
};

// Models Constants
module.exports.MODEL_CONSTANTS = {
  // Token status
  STATUS_ACTIVE: "active",
  STATUS_EXPIRED: "expired",

  // Token Scheduler States
  NOT_STARTED: "NOT_STARTED",
  RUNNING: "RUNNING",
  PAUSED: "PAUSED",
  COMPLETED: "COMPLETED",

  // SmartApi Modes
  MODE_FULL: "FULL",
  MODE_LTP: "LTP",
  MODE_OHLC: "OHLC",
  MODE_NONE: "NONE",
};
