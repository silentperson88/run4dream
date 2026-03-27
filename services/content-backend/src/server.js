require("dotenv").config();
const app = require("./app");

const port = Number(process.env.PORT || 8003);
const server = app.listen(port, "0.0.0.0", () => {
  console.log(`Content service running on port ${port}`);
});

// Keep the process alive even if a runtime/watch quirk drops the server handle.
// This is intentionally unref'ed nowhere; the service should stay up until the
// process is explicitly stopped.
const keepAliveTimer = setInterval(() => {}, 60_000);

process.on("exit", (code) => {
  try {
    clearInterval(keepAliveTimer);
    server?.close?.();
    console.log(`Content service exiting with code ${code}`);
  } catch (_) {
    // no-op
  }
});

process.on("uncaughtException", (err) => {
  console.error("Content service uncaught exception:", err);
});

process.on("unhandledRejection", (err) => {
  console.error("Content service unhandled rejection:", err);
});
