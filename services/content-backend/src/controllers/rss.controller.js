const { response } = require("../utils/response.utils");
const { fetchRss } = require("../services/rss.service");
const { saveRssItem, listRssItems, getRssProgress, getRssByLink, linkVideoById, markProcessedById, updateRssItemDraft } = require("../services/rssSaved.service");
const { createVideo } = require("../services/newsContentVideos.service");
const { chatWithOllama } = require("../services/ollama.service");
const { generateGeminiText } = require("../services/gemini.service");
const socialTemplatePrompts = require("../config/social-template-prompts.json");

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

function limitWords(text, maxWords = 24) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  if (!words.length) return "";
  if (words.length <= maxWords) return words.join(" ");
  return `${words.slice(0, maxWords).join(" ")}…`;
}

function limitChars(text, maxChars = 160) {
  const value = String(text || "").trim();
  if (value.length <= maxChars) return value;
  return `${value.slice(0, Math.max(0, maxChars - 1)).trim()}…`;
}

function pickSentenceBlock(text, startIndex, count) {
  const sentences = splitSentences(text);
  if (!sentences.length) return "";
  return sentences.slice(startIndex, startIndex + count).join(". ").trim();
}

function buildShortText(text, maxWords = 24) {
  const fromSentences = pickSentenceBlock(text, 0, 2);
  return limitWords(fromSentences || text, maxWords);
}

function stripCodeFence(text) {
  const raw = String(text || "").trim();
  if (!raw) return raw;
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced?.[1]) return fenced[1].trim();
  return raw;
}

function extractJsonCandidate(text) {
  const direct = stripCodeFence(text);
  if (!direct) return "";
  if (direct.startsWith("{") || direct.startsWith("[")) return direct;

  const firstObj = direct.indexOf("{");
  const lastObj = direct.lastIndexOf("}");
  const firstArr = direct.indexOf("[");
  const lastArr = direct.lastIndexOf("]");

  const objectCandidate =
    firstObj >= 0 && lastObj > firstObj ? direct.slice(firstObj, lastObj + 1) : "";
  const arrayCandidate =
    firstArr >= 0 && lastArr > firstArr ? direct.slice(firstArr, lastArr + 1) : "";

  return arrayCandidate.length > objectCandidate.length ? arrayCandidate : objectCandidate;
}

function safeParseJson(text) {
  const candidate = extractJsonCandidate(text);
  if (!candidate) return null;
  try {
    return JSON.parse(candidate);
  } catch (_) {
    return null;
  }
}

function buildMediumText(text, maxWords = 42) {
  const fromSentences = pickSentenceBlock(text, 0, 4);
  return limitWords(fromSentences || text, maxWords);
}

function normalizeBullets(value, fallbackText, limit = 4) {
  const list = normalizeList(value, limit);
  if (list.length) return list.map(item => limitWords(item, 12)).filter(Boolean);
  return fallbackHighlights(fallbackText, limit).map(item => limitWords(item, 12)).filter(Boolean);
}

function normalizePages(pages, { title, cleanedText, images }) {
  const imagePool = Array.isArray(images) ? images.map(item => String(item || "").trim()).filter(Boolean) : [];
  const fallbackSentences = splitSentences(cleanedText);
  const fallbackPages = [
    {
      pageType: "cover",
      title: String(title || "Top Story").trim(),
      subtitle: limitChars(fallbackSentences[0] || cleanedText || title || "A quick social-ready summary", 90),
      shortText: buildShortText(cleanedText, 26),
      mediumText: buildMediumText(cleanedText, 48),
      bullets: fallbackHighlights(cleanedText, 3).map(item => limitWords(item, 10)).filter(Boolean),
      cta: "Swipe for more",
      image: imagePool[0] || null,
    },
    {
      pageType: "detail",
      title: limitChars(fallbackSentences[1] || fallbackSentences[0] || title || "Key update", 64),
      subtitle: limitChars(fallbackSentences[2] || fallbackSentences[1] || "", 90),
      shortText: buildShortText(cleanedText, 24),
      mediumText: buildMediumText(cleanedText, 42),
      bullets: fallbackHighlights(cleanedText, 4).slice(0, 4).map(item => limitWords(item, 10)).filter(Boolean),
      cta: "Read the full story",
      image: imagePool[1] || imagePool[0] || null,
    },
    {
      pageType: "takeaway",
      title: "What it means",
      subtitle: limitChars(fallbackSentences[3] || fallbackSentences[2] || "", 90),
      shortText: buildShortText(cleanedText, 22),
      mediumText: buildMediumText(cleanedText, 40),
      bullets: fallbackHighlights(cleanedText, 3).slice(0, 3).map(item => limitWords(item, 10)).filter(Boolean),
      cta: "Save this update",
      image: imagePool[2] || imagePool[0] || null,
    },
  ];

  const safePages = Array.isArray(pages) && pages.length ? pages : fallbackPages;
  return safePages.slice(0, 5).map((page, index) => {
    const pageType = String(page?.pageType || page?.type || page?.layout || ["cover", "detail", "takeaway", "stat", "quote"][index] || "page").trim();
    const pageTitle = String(page?.title || page?.heading || page?.headline || fallbackPages[index]?.title || title || `Page ${index + 1}`).trim();
    const subtitle = limitChars(page?.subtitle || page?.dek || page?.description || fallbackPages[index]?.subtitle || "", 120);
    const shortText = limitChars(page?.shortText || page?.bodyShort || page?.body || buildShortText(cleanedText, 26), 220);
    const mediumText = limitChars(page?.mediumText || page?.bodyLong || buildMediumText(cleanedText, 48), 360);
    const bullets = normalizeBullets(page?.bullets || page?.highlights || page?.points, cleanedText, 4);
    const cta = String(page?.cta || fallbackPages[index]?.cta || "").trim();
    const imageIndex = Number.isInteger(Number(page?.imageIndex)) ? Number(page?.imageIndex) : index;
    const image = imagePool.length ? imagePool[Math.max(0, Math.min(imagePool.length - 1, imageIndex))] || null : null;
    return {
      pageType,
      title: pageTitle,
      subtitle,
      shortText,
      mediumText,
      bullets,
      cta: cta || "Swipe for more",
      image,
    };
  });
}

function normalizeImprovedTemplate(payload, templateType) {
  const kind = String(templateType || "").trim();
  if (kind === "templateOne") {
    return {
      title: limitWords(String(payload?.title || "").trim(), 3),
      subtitle: limitChars(String(payload?.subtitle || payload?.shortText || "").trim(), 100),
      shortText: limitChars(String(payload?.shortText || payload?.subtitle || "").trim(), 220),
      mediumText: limitChars(String(payload?.mediumText || payload?.shortText || payload?.subtitle || "").trim(), 360),
      highlights: normalizeList(payload?.highlights, 4),
      cta: "",
    };
  }

  if (kind === "templateTwo") {
    return {
      heading: normalizeHeading(payload?.heading, [], 4),
      bullets: normalizeBullets(payload?.bullets, "", 4),
      shortText: limitChars(String(payload?.shortText || "").trim(), 220),
      mediumText: limitChars(String(payload?.mediumText || payload?.shortText || "").trim(), 360),
      image: payload?.image ? String(payload.image).trim() : null,
    };
  }

  return {
    title: limitWords(String(payload?.title || "").trim(), 3),
    subtitle: limitChars(String(payload?.subtitle || payload?.kicker || "").trim(), 100),
    kicker: limitChars(String(payload?.kicker || "").trim(), 60),
    image: payload?.image ? String(payload.image).trim() : null,
  };
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
    "You are a social media news editor. Generate short post content for Instagram/Facebook based on the news. Output only valid JSON with keys templateOne, templateTwo, templateThree, pages.\n\n" +
    "templateOne fields: title (maximum 3 words), subtitle (1 short sentence, max 100 characters including spaces), shortText (same idea as subtitle, short paragraph), mediumText (a slightly longer paragraph, about 1.5x shortText), highlights (3-4 short bullets, each 6-8 words max), cta (short).\n" +
    "templateTwo fields: heading (2-4 short lines, each line <= 7 words), bullets (2-4 bullets), shortText, mediumText.\n" +
    "templateThree fields: title (short, header only), subtitle (optional), kicker (optional).\n" +
    "pages fields: array of 3-5 page objects. Each page can include pageType, title, subtitle, shortText, mediumText, bullets, cta, imageIndex.\n\n" +
    "Rules: Use only news facts from the text. Do not add commentary. Keep sentences concise. No hashtags. No quotes. No extra keys.\n" +
    "Keep the tone social-first and publishable.\n\n" +
    "Title:\n" +
    title +
    "\n\nNews Text:\n" +
    cleanedText;

  let templateOne = null;
  let templateTwo = null;
  let templateThree = null;
  let pages = null;
  try {
    const result = await chatWithOllama({ prompt, format: "json" });
    const raw = String(result?.response || "").trim();
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && typeof parsed === "object") {
      templateOne = parsed.templateOne || parsed.template_one || null;
      templateTwo = parsed.templateTwo || parsed.template_two || null;
      templateThree = parsed.templateThree || parsed.template_three || null;
      pages = parsed.pages || parsed.templatePages || parsed.template_pages || null;
    }
  } catch (_) {
    templateOne = null;
    templateTwo = null;
    templateThree = null;
    pages = null;
  }

  const safeHighlights = fallbackHighlights(cleanedText, 4);
  const safeMedium = buildMediumText(cleanedText, 42);
  const safeShort = buildShortText(cleanedText, 26);
  const imageOne = Array.isArray(images) && images.length ? String(images[0]) : null;
  const imageTwo =
    Array.isArray(images) && images.length > 1 ? String(images[1]) : imageOne;

  const normalizedOne = {
    title: limitWords(String(templateOne?.title || title || "Top Story").trim(), 3),
    subtitle: limitChars(String(templateOne?.subtitle || templateOne?.shortText || safeHighlights[0] || "").trim(), 100),
    shortText: limitChars(templateOne?.shortText || safeShort, 220),
    mediumText: limitChars(templateOne?.mediumText || safeMedium, 360),
    highlights: (normalizeList(templateOne?.highlights, 4).length
      ? normalizeList(templateOne?.highlights, 4)
      : safeHighlights.slice(0, 4)
    ).filter(item => isWithinWordLimit(item, 15)),
    cta: "",
    image: imageOne,
  };

  const normalizedTwo = {
    heading: normalizeHeading(templateTwo?.heading, safeHighlights, 4),
    bullets: normalizeBullets(templateTwo?.bullets, cleanedText, 4),
    shortText: limitChars(templateTwo?.shortText || safeShort, 220),
    mediumText: limitChars(templateTwo?.mediumText || safeMedium, 360),
    image: imageTwo,
  };

  const normalizedThree = {
    title: String(templateThree?.title || title || "Top Story").trim(),
    subtitle: String(templateThree?.subtitle || safeHighlights[0] || "").trim(),
    kicker: String(templateThree?.kicker || safeHighlights[1] || "").trim(),
    image: imageOne,
  };

  const normalizedPages = normalizePages(pages, { title, cleanedText, images });

  return {
    templateOne: normalizedOne,
    templateTwo: normalizedTwo,
    templateThree: normalizedThree,
    pages: normalizedPages,
  };
}

async function improveSocialTemplate(req, res) {
  try {
    const templateType = String(req.body?.templateType || "").trim();
    if (!["templateOne", "templateTwo", "templateThree"].includes(templateType)) {
      return response(res, 400, "templateType must be templateOne, templateTwo, or templateThree");
    }

    const title = String(req.body?.title || req.body?.currentTitle || req.body?.newsTitle || "").trim();
    const cleanedText = String(req.body?.cleanedText || req.body?.rawText || req.body?.articleText || "").trim();
    const model = String(req.body?.model || process.env.GEMINI_MODEL || "gemini-2.5-flash").trim();
    const current = req.body?.current || {};
    const images = Array.isArray(req.body?.images) ? req.body.images.map((item) => String(item || "").trim()).filter(Boolean) : [];

    const templateMeta = socialTemplatePrompts?.[templateType] || {};
    const prompt = `
${templateMeta?.geminiPrompt || "Rewrite the template in a social-first way."}

Return ONLY valid JSON.

Template type: ${templateType}
Template notes: ${JSON.stringify(templateMeta)}

News title:
${title}

News text:
${cleanedText}

Selected images in order:
${JSON.stringify(images)}

Current draft:
${JSON.stringify(current)}

Expected JSON shape:
${templateType === "templateOne" ? `{
  "title": "max 3 words",
  "subtitle": "max 100 characters",
  "shortText": "short sentence",
  "mediumText": "slightly longer sentence",
  "highlights": ["bullet 1", "bullet 2", "bullet 3"],
  "cta": ""
}` : templateType === "templateTwo" ? `{
  "heading": ["short line 1", "short line 2"],
  "bullets": ["bullet 1", "bullet 2", "bullet 3"],
  "shortText": "short line",
  "mediumText": "slightly longer line"
}` : `{
  "title": "max 3 words",
  "subtitle": "one short sentence",
  "kicker": "short kicker"
}`}
`.trim();

    const result = await generateGeminiText({ prompt, model, temperature: 0.2 });
    const parsed = safeParseJson(result?.text || result?.response || "");
    if (!parsed) {
      return response(res, 400, "Gemini did not return valid JSON");
    }

    const improved = normalizeImprovedTemplate(parsed, templateType);
    return response(res, 200, "Template improved", {
      templateType,
      improved,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to improve template");
  }
}

async function getTemplatePrompts(req, res) {
  try {
    return response(res, 200, "Template prompts", socialTemplatePrompts);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to load template prompts");
  }
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

async function updateRssDraft(req, res) {
  try {
    const id = Number(req.params?.id || req.body?.id || 0);
    if (!id) return response(res, 400, "id is required");
    const images = Array.isArray(req.body?.images) ? req.body.images.map((item) => String(item || "").trim()).filter(Boolean) : null;
    const templateImageItems = Array.isArray(req.body?.templateImageItems) ? req.body.templateImageItems : null;
    const cleanedText = typeof req.body?.cleanedText === "string" ? req.body.cleanedText : null;
    const templateOne = req.body?.templateOne || null;
    const templateTwo = req.body?.templateTwo || null;
    const templateThree = req.body?.templateThree || null;
    const templatePages = Array.isArray(req.body?.templatePages) ? req.body.templatePages : null;
    const templateMusicSelection = req.body?.templateMusicSelection || null;
    const updated = await updateRssItemDraft({
      id,
      images,
      templateImageItems,
      cleanedText,
      templateOne,
      templateTwo,
      templateThree,
      templatePages,
      templateMusicSelection,
    });
    if (!updated) return response(res, 404, "RSS item not found");
    return response(res, 200, "RSS draft updated", updated);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to update RSS draft");
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
      templatePages: templates?.pages || null,
      error: null,
    });
    return response(res, 200, "Processed", {
      id: existing.id,
      cleanedText,
      templateOne: templates?.templateOne || null,
      templateTwo: templates?.templateTwo || null,
      templateThree: templates?.templateThree || null,
      pages: templates?.pages || null,
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

module.exports = { getRss, saveRss, updateRssDraft, listRss, progressRss, processOne, fetchArticleBody, linkNewsContentVideo, improveSocialTemplate, getTemplatePrompts };
