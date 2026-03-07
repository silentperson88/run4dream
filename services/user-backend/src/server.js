require("dotenv").config();
const { dbReady } = require("./config/db");

const start = async () => {
  await dbReady;
  const app = require("./app");
  const port = Number(process.env.PORT || 8001);
  app.listen(port, "0.0.0.0", () => {
    console.log(`User service running on port ${port}`);
  });
};

start().catch((err) => {
  console.error("User service failed to start", err?.message || err);
  process.exit(1);
});
