require("dotenv").config();
const { dbReady } = require("./config/db");

const start = async () => {
  await dbReady;
  const PORT = process.argv[2] || process.env.PORT || 8000;
  const app = require("./app");
  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log("Server is running on port %s", PORT);
  });
  module.exports = server;
};

start().catch((err) => {
  console.error("Market service failed to start", err?.message || err);
  process.exit(1);
});
