const { lockNextPending, markInProgress, markCompleted, markFailed } = require("../src/services/rssSaved.service");
const { chatWithOllama } = require("../src/services/ollama.service");

const POLL_MS = Number(process.env.RSS_WORKER_POLL_MS || 5000);
const MAX_IDLE_MS = Number(process.env.RSS_WORKER_IDLE_SLEEP_MS || 2000);

function stripTags(html) {
  return String(html || "").replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractArticleBody(html) {
  const src = String(html || "");
  const tagRegex = /<\/?div\b[^>]*>/gi;
  let match = null;
  let startIndex = -1;
  let depth = 0;
  while ((match = tagRegex.exec(src))) {
    const tag = match[0];
    const isClose = tag.startsWith("</");
    if (startIndex === -1) {
      if (!isClose && /data-articlebody\s*=\s*["']?1["']?/i.test(tag)) {
        startIndex = tagRegex.lastIndex;
        depth = 1;
      }
      continue;
    }
    if (!isClose) depth += 1;
    else depth -= 1;
    if (depth === 0 && startIndex !== -1) {
      return src.slice(startIndex, match.index);
    }
  }
  return "";
}

function extractReadableText(html) {
  const text = stripTags(extractArticleBody(html) || html);
  return text.slice(0, 20000);
}

async function scrapeUrl(url) {
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (!res.ok) throw new Error(`Failed to fetch article (${res.status})`);
  const html = await res.text();
  return extractReadableText(html);
}

async function rewriteWithOllama({ title, rawText }) {
  const prompt =
    "You are a professional news editor. Given a news title and raw article text, remove ads, boilerplate, navigation, and unrelated content. Then rewrite the news into a detailed, well-structured article in English (at least 8-12 short paragraphs). Use the title to decide what to keep. Do not add new facts. Do not add meta commentary, disclaimers, or filler like 'Here is' or 'Okay'. Output only the news text.\n\nTitle:\n" +
    title +
    "\n\nRaw Text:\n" +
    rawText;
  const result = await chatWithOllama({ prompt });
  return String(result?.response || "").trim();
}

async function processOnce() {
  const job = await lockNextPending();
  if (!job) return false;
  try {
    const rawText = await scrapeUrl(job.link);
    await markInProgress({ id: job.id, rawText });
    const cleaned = await rewriteWithOllama({ title: job.title, rawText });
    await markCompleted({ id: job.id, cleanedText: cleaned });
  } catch (err) {
    await markFailed({ id: job.id, error: err?.message || "Failed" });
  }
  return true;
}

async function main() {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const worked = await processOnce();
      if (!worked) {
        await new Promise((r) => setTimeout(r, MAX_IDLE_MS));
      }
    } catch (_) {
      await new Promise((r) => setTimeout(r, POLL_MS));
    }
  }
}

main().catch((err) => {
  console.error("RSS worker failed", err);
  process.exit(1);
});
