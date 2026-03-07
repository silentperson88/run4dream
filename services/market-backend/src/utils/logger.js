const logger = {
  info: (...args) => console.log("ℹ️", ...args),
  error: (...args) => console.error("❌", ...args),
};

export default logger;
