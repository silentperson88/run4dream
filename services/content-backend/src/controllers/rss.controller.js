const { response } = require("../utils/response.utils");
const { fetchRss } = require("../services/rss.service");
const { saveRssItem, listRssItems, getRssProgress, getRssByLink, linkVideoById, markProcessedById } = require("../services/rssSaved.service");
const { createVideo } = require("../services/newsContentVideos.service");
const { chatWithOllama } = require("../services/ollama.service");

function stripTags(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
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

function getArticleBodyText(html) {
  return stripTags(extractArticleBody(html));
}

function getArticleBodyImages(html) {
  const bodyHtml = extractArticleBody(html);
  const images = [];
  if (bodyHtml) {
    const imgRegex = /<img[^>]*src=["']([^"']+)["'][^>]*>/gi;
    let match = null;
    while ((match = imgRegex.exec(bodyHtml))) {
      const src = String(match[1] || "").trim();
      if (src) images.push(src);
    }
  }
  return images;
}

function normalizeList(value, limit) {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => String(item || "").trim())
    .filter(Boolean)
    .slice(0, limit);
}

function normalizeHeading(value, fallback, limit = 4) {
  if (Array.isArray(value)) {
    const list = normalizeList(value, limit);
    if (list.length) return list;
  }
  const raw = String(value || "").trim();
  if (raw) {
    const parts = raw.split(/\r?\n/).map(item => item.trim()).filter(Boolean);
    if (parts.length) return parts.slice(0, limit);
    const sentences = splitSentences(raw);
    if (sentences.length) return sentences.slice(0, limit);
  }
  return fallback.slice(0, limit);
}

function splitSentences(text) {
  return String(text || "")
    .replace(/\s+/g, " ")
    .split(/[.!?]+/g)
    .map(item => item.trim())
    .filter(Boolean);
}

function buildParagraphs(text, minSentences = 8) {
  const sentences = splitSentences(text);
  if (!sentences.length) return "";
  const target = sentences.length >= minSentences ? sentences : sentences.slice(0, minSentences);
  const paragraphs = [];
  for (let i = 0; i < target.length; i += 3) {
    paragraphs.push(target.slice(i, i + 3).join(". ") + ".");
  }
  return paragraphs.join("\n\n").trim();
}

function fallbackHighlights(text, limit = 4) {
  const sentences = splitSentences(text);
  return sentences.slice(0, limit).map(item => item.slice(0, 120));
}

function isWithinWordLimit(text, maxWords = 15) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  return words.length > 0 && words.length <= maxWords;
}

async function fetchArticleHtml(url) {
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (!res.ok) throw new Error(`Failed to fetch article (${res.status})`);
  return await res.text();
}

async function rewriteWithOllama({ title, rawText }) {
  const prompt =
    "You are a professional news editor.\n\n" +
    "Task: Given a news title and raw article text, remove ads, boilerplate, navigation, and unrelated content. Rewrite the news into a detailed, well-structured article in English with 8-12 short paragraphs. Use the title to decide what to keep. Do not add new facts. Do not add meta commentary, disclaimers, or filler like 'Here is' or 'Okay'.\n\n" +
    "Output rules:\n" +
    "- Return ONLY the rewritten article as plain text paragraphs.\n" +
    "- Do NOT return JSON.\n" +
    "- Do NOT return a title line or any labels.\n\n" +
    "Title:\n" +
    title +
    "\n\nRaw Text:\n" +
    rawText;
  const result = await chatWithOllama({ prompt });
  const rawResponse = result?.response;
  if (rawResponse && typeof rawResponse === "object") {
    let newsValue = rawResponse.news ?? rawResponse.text ?? rawResponse.content ?? rawResponse.body ?? rawResponse;
    if (newsValue && typeof newsValue === "object") {
      newsValue = newsValue.text || newsValue.content || newsValue.body || "";
    }
    const news = String(newsValue || "").trim();
    if (news) return buildParagraphs(news) || news;
  }
  let raw = String(rawResponse || "").trim();
  if (raw.startsWith("{") || raw.startsWith("[")) {
    try {
      const parsed = JSON.parse(raw);
      let newsValue = parsed?.news ?? parsed?.text ?? parsed?.content ?? parsed?.body ?? parsed?.title ?? "";
      if (newsValue && typeof newsValue === "object") {
        newsValue = newsValue.text || newsValue.content || newsValue.body || "";
      }
      const jsonText = String(newsValue || "").trim();
      if (jsonText) raw = jsonText;
    } catch (_) {
      // keep raw
    }
  }
  try {
    const parsed = raw ? JSON.parse(raw) : null;
    let newsValue = parsed?.news;
    if (newsValue && typeof newsValue === "object") {
      newsValue = newsValue.text || newsValue.content || newsValue.body || "";
    }
    const news = String(newsValue || "").trim();
    if (news) return buildParagraphs(news) || news;
  } catch (_) {
    // fall through
  }
  const cleaned = raw.replace(/^["']?|["']?$/g, "").trim();
  if (cleaned) return buildParagraphs(cleaned) || cleaned;
  return buildParagraphs(rawText, 8) || "";
}

async function generateSocialTemplates({ title, cleanedText, images }) {
  const prompt =
    "You are a social media news editor. Generate short post content for Instagram/Facebook based on the news. Output only valid JSON with keys templateOne, templateTwo, templateThree.\n\n" +
    "templateOne fields: title (short), subtitle (1 line), highlights (3-4 short bullets, each 6-8 words max), cta (short).\n" +
    "templateTwo fields: heading (2-4 short lines, each line <= 7 words).\n" +
    "templateThree fields: title (short, header only).\n\n" +
    "Rules: Use only news facts from the text. Do not add commentary. Keep sentences concise. No hashtags. No quotes. No extra keys.\n\n" +
    "Title:\n" +
    title +
    "\n\nNews Text:\n" +
    cleanedText;

  let templateOne = null;
  let templateTwo = null;
  let templateThree = null;
  try {
    const result = await chatWithOllama({ prompt, format: "json" });
    const raw = String(result?.response || "").trim();
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && typeof parsed === "object") {
      templateOne = parsed.templateOne || parsed.template_one || null;
      templateTwo = parsed.templateTwo || parsed.template_two || null;
      templateThree = parsed.templateThree || parsed.template_three || null;
    }
  } catch (_) {
    templateOne = null;
    templateTwo = null;
    templateThree = null;
  }

  const safeHighlights = fallbackHighlights(cleanedText, 4);
  const imageOne = Array.isArray(images) && images.length ? String(images[0]) : null;
  const imageTwo =
    Array.isArray(images) && images.length > 1 ? String(images[1]) : imageOne;

  const normalizedOne = {
    title: String(templateOne?.title || title || "Top Story").trim(),
    subtitle: String(templateOne?.subtitle || safeHighlights[0] || "").trim(),
    highlights: (normalizeList(templateOne?.highlights, 4).length
      ? normalizeList(templateOne?.highlights, 4)
      : safeHighlights.slice(0, 4)
    ).filter(item => isWithinWordLimit(item, 15)),
    cta: String(templateOne?.cta || "See more >>").trim(),
    image: imageOne,
  };

  const normalizedTwo = {
    heading: normalizeHeading(templateTwo?.heading, safeHighlights, 4),
    image: imageTwo,
  };

  const normalizedThree = {
    title: String(templateThree?.title || title || "Top Story").trim(),
    image: imageOne,
  };

  return { templateOne: normalizedOne, templateTwo: normalizedTwo, templateThree: normalizedThree };
}

async function getRss(req, res) {
  try {
    const source = String(req.query?.source || "").toLowerCase();
    if (!source) return response(res, 400, "source is required");
    const items = await fetchRss(source);
    return response(res, 200, "RSS items", items);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch RSS");
  }
}

async function saveRss(req, res) {
  try {
    const payload = {
      userId: Number(req.user?.id || 0),
      source: req.body?.source,
      title: req.body?.title,
      link: req.body?.link,
      pubDate: req.body?.pubDate,
    };
    const saved = await saveRssItem(payload);
    return response(res, 200, saved ? "Saved" : "Already saved", {
      saved: Boolean(saved),
      item: saved,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to save RSS item");
  }
}

async function listRss(req, res) {
  try {
    const data = await listRssItems({
      userId: Number(req.user?.id || 0),
      limit: req.query?.limit || 100,
      offset: req.query?.offset || 0,
      fromDate: req.query?.fromDate,
      toDate: req.query?.toDate,
    });
    return response(res, 200, "RSS saved items", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to list RSS items");
  }
}

async function progressRss(req, res) {
  try {
    const data = await getRssProgress({
      userId: Number(req.user?.id || 0),
      fromDate: req.query?.fromDate,
      toDate: req.query?.toDate,
    });
    return response(res, 200, "RSS progress", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to get RSS progress");
  }
}

async function processOne(req, res) {
  try {
    const link = String(req.body?.link || "").trim();
    if (!link) return response(res, 400, "link is required");
    const existing = await getRssByLink({ link });
    if (!existing) return response(res, 404, "News link not found. Save it first.");

    const html = await fetchArticleHtml(link);
    const rawText = getArticleBodyText(html).slice(0, 20000);
    const images = html ? getArticleBodyImages(html) : [];
    const cleanedText = await rewriteWithOllama({
      title: String(existing.title || ""),
      rawText,
    });
    const templates = await generateSocialTemplates({
      title: String(existing.title || ""),
      cleanedText,
      images,
    });
    await markProcessedById({
      id: existing.id,
      status: "completed",
      rawText,
      cleanedText,
      images,
      templateOne: templates?.templateOne || null,
      templateTwo: templates?.templateTwo || null,
      templateThree: templates?.templateThree || null,
      error: null,
    });
    return response(res, 200, "Processed", {
      id: existing.id,
      cleanedText,
      templateOne: templates?.templateOne || null,
      templateTwo: templates?.templateTwo || null,
      templateThree: templates?.templateThree || null,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to process news");
  }
}

async function fetchArticleBody(req, res) {
  try {
    const link = String(req.body?.link || "").trim();
    if (!link) return response(res, 400, "link is required");
    const resFetch = await fetch(link, { headers: { "User-Agent": "Mozilla/5.0" } });
    if (!resFetch.ok) throw new Error(`Failed to fetch article (${resFetch.status})`);
    const html = await resFetch.text();
    const rawText = getArticleBodyText(html);
    const images = getArticleBodyImages(html);
    return response(res, 200, "Article body text", { rawText, images });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch article body");
  }
}

async function linkNewsContentVideo(req, res) {
  try {
    const link = String(req.body?.link || "").trim();
    if (!link) return response(res, 400, "link is required");
    const existing = await getRssByLink({ link });
    if (!existing) return response(res, 404, "News link not found. Save it first.");

    if (existing.news_content_video_id) {
      return response(res, 200, "Already linked", {
        videoId: existing.news_content_video_id,
        created: false,
      });
    }

    const video = await createVideo({ userId: Number(req.user?.id || 0), language: "english" });
    if (!video?.id) throw new Error("Unable to create video");
    await linkVideoById({ id: existing.id, videoId: video.id });
    return response(res, 200, "Linked", { videoId: video.id, created: true });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to link news content video");
  }
}

module.exports = { getRss, saveRss, listRss, progressRss, processOne, fetchArticleBody, linkNewsContentVideo };
