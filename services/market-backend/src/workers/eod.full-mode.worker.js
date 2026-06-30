require("dotenv").config();
require("../config/db");

const { syncDailyEodFromFullMode } = require("../services/stockOhlcEod.service");

const argv = process.argv.slice(2);
const readArg = (...names) => {
  for (const name of names) {
    const prefix = `--${name}=`;
    const hit = argv.find((arg) => arg.startsWith(prefix));
    if (hit) return hit.slice(prefix.length);
  }
  return null;
};

const runtime = {
  masterId: Number(readArg("master-id") || 0) || null,
};

async function run() {
  console.log("Starting FULL mode daily EOD worker", {
    masterId: runtime.masterId,
  });

  const result = await syncDailyEodFromFullMode({
    masterId: runtime.masterId,
  });

  console.log("FULL mode daily EOD worker completed", result);
}

run().catch((error) => {
  console.error("Fatal FULL mode daily EOD worker error:", error?.message || error);
  process.exitCode = 1;
});
