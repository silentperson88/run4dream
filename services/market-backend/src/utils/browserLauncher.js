const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const candidatePaths = () => {
  const candidates = [];

  const envPaths = [
    process.env.PLAYWRIGHT_EXECUTABLE_PATH,
    process.env.PLAYWRIGHT_CHROMIUM_PATH,
    process.env.CHROME_BIN,
    process.env.CHROME_PATH,
  ].filter(Boolean);
  candidates.push(...envPaths);

  const localAppData = process.env.LOCALAPPDATA;
  const programFiles = process.env.PROGRAMFILES;
  const programFilesX86 = process.env["PROGRAMFILES(X86)"];

  if (localAppData) {
    candidates.push(
      path.join(localAppData, "Google", "Chrome", "Application", "chrome.exe"),
      path.join(localAppData, "Microsoft", "Edge", "Application", "msedge.exe"),
    );
  }

  if (programFiles) {
    candidates.push(
      path.join(programFiles, "Google", "Chrome", "Application", "chrome.exe"),
      path.join(programFiles, "Microsoft", "Edge", "Application", "msedge.exe"),
    );
  }

  if (programFilesX86) {
    candidates.push(
      path.join(programFilesX86, "Google", "Chrome", "Application", "chrome.exe"),
      path.join(programFilesX86, "Microsoft", "Edge", "Application", "msedge.exe"),
    );
  }

  return candidates.filter(Boolean);
};

async function launchChromium(options = {}) {
  const launchOptions = { headless: true, ...options };

  for (const executablePath of candidatePaths()) {
    if (!fs.existsSync(executablePath)) continue;
    try {
      return await chromium.launch({
        ...launchOptions,
        executablePath,
      });
    } catch (error) {
      // Try next candidate
    }
  }

  try {
    return await chromium.launch(launchOptions);
  } catch (error) {
    const message = String(error?.message || error || "");
    if (
      message.includes("Executable doesn't exist") ||
      message.includes("browserType.launch")
    ) {
      try {
        return await chromium.launch({
          ...launchOptions,
          channel: "chrome",
        });
      } catch (chromeError) {
        throw chromeError;
      }
    }
    throw error;
  }
}

module.exports = {
  launchChromium,
};
