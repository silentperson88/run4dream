const { response } = require("../utils/response.utils");
const newsIngestService = require("../services/newsIngest.service");

async function fetchAnnouncements(req, res) {
  try {
    const result = await newsIngestService.fetchAndStoreNews({
      userId: Number(req.user?.id || 0),
      date: req.body?.date,
      category: req.body?.category,
      scrip: req.body?.scrip,
      search: req.body?.search,
      annType: req.body?.annType,
      subcategory: req.body?.subcategory,
      minScore: req.body?.minScore,
      maxPages: req.body?.maxPages,
      timeoutMs: req.body?.timeoutMs,
      extraKeywords: req.body?.extraKeywords,
      limit: req.body?.limit,
      offset: req.body?.offset,
    });
    return response(res, 200, "BSE announcements fetched and stored", result);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch announcements");
  }
}

async function getNewsList(req, res) {
  try {
    const result = await newsIngestService.getNewsList({
      userId: Number(req.user?.id || 0),
      limit: req.query?.limit,
      offset: req.query?.offset,
      date: req.query?.date,
      category: req.query?.category,
      matchStatus: req.query?.matchStatus,
      excludeUsedForVideo: req.query?.excludeUsedForVideo,
    });
    return response(res, 200, "BSE news list", result);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch news list");
  }
}

async function getNewsCategories(req, res) {
  try {
    const categories = await newsIngestService.getNewsCategories({
      userId: Number(req.user?.id || 0),
    });
    return response(res, 200, "BSE categories list", {
      rows: categories,
    });
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch categories list");
  }
}

async function getNewsItem(req, res) {
  try {
    const result = await newsIngestService.getNewsItem({
      userId: Number(req.user?.id || 0),
      id: req.params.id,
    });
    return response(res, 200, "BSE news item", result);
  } catch (err) {
    return response(res, 404, err?.message || "News not found");
  }
}

async function createFullVideoRecord(req, res) {
  try {
    const result = await newsIngestService.createFullVideoRecord({
      userId: Number(req.user?.id || 0),
      date: req.body?.date,
      category: req.body?.category,
      title: req.body?.title,
      renderJobId: req.body?.renderJobId,
      fileName: req.body?.fileName,
      videoUrl: req.body?.videoUrl,
      status: req.body?.status,
      newsRows: req.body?.newsRows,
    });
    return response(res, 201, "Full video record stored", result);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to store full video record");
  }
}

async function listFullVideos(req, res) {
  try {
    const result = await newsIngestService.listFullVideos({
      userId: Number(req.user?.id || 0),
      date: req.query?.date,
      limit: req.query?.limit,
      offset: req.query?.offset,
    });
    return response(res, 200, "Full videos list", result);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to fetch full videos list");
  }
}

module.exports = {
  fetchAnnouncements,
  getNewsList,
  getNewsItem,
  getNewsCategories,
  createFullVideoRecord,
  listFullVideos,
};
