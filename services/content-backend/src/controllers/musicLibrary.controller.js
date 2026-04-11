const fs = require("fs");
const { response } = require("../utils/response.utils");
const {
  listCategories,
  createCategory,
  listTracks,
  uploadTrack,
  updateTrack,
  getTrackFilePath,
} = require("../services/musicLibrary.service");

async function getMusicCategories(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const categories = await listCategories({ userId });
    return response(res, 200, "Music categories", categories);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to load music categories");
  }
}

async function createMusicCategory(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const categoryName = String(req.body?.categoryName || "").trim();
    if (!categoryName) return response(res, 400, "categoryName is required");
    const created = await createCategory({ userId, categoryName });
    return response(res, 200, "Music category created", created);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to create music category");
  }
}

async function getMusicTracks(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const search = String(req.query?.search || "").trim();
    const categoryId = req.query?.categoryId ? Number(req.query?.categoryId) : null;
    const tracks = await listTracks({ userId, search, categoryId });
    return response(res, 200, "Music tracks", tracks);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to load music tracks");
  }
}

async function uploadMusicTrack(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const title = String(req.body?.title || "").trim();
    const mediaType = String(req.body?.mediaType || "music").trim().toLowerCase();
    const fileName = String(req.body?.fileName || "").trim();
    const dataUrl = typeof req.body?.dataUrl === "string" ? req.body.dataUrl : null;
    const sourceUrl = typeof req.body?.sourceUrl === "string" ? req.body.sourceUrl : null;
    const categoryIds = Array.isArray(req.body?.categoryIds) ? req.body.categoryIds : [];
    if (!fileName) return response(res, 400, "fileName is required");
    if (!dataUrl && !sourceUrl) return response(res, 400, "dataUrl or sourceUrl is required");
    const created = await uploadTrack({ userId, title, mediaType, fileName, dataUrl, sourceUrl, categoryIds });
    return response(res, 200, "Music track uploaded", created);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to upload music track");
  }
}

async function updateMusicTrack(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const id = Number(req.params?.id || 0);
    if (!id) return response(res, 400, "id is required");
    const title = typeof req.body?.title === "string" ? req.body.title : null;
    const categoryIds = Array.isArray(req.body?.categoryIds) ? req.body.categoryIds : [];
    const updated = await updateTrack({ userId, id, title, categoryIds });
    if (!updated) return response(res, 404, "Music track not found");
    return response(res, 200, "Music track updated", updated);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to update music track");
  }
}

async function streamMusicFile(req, res) {
  try {
    const fileName = String(req.params?.fileName || "").trim();
    if (!fileName) return response(res, 400, "fileName is required");
    const filePath = getTrackFilePath(fileName);
    if (!fs.existsSync(filePath)) return response(res, 404, "Music file not found");
    return res.sendFile(filePath);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to stream music file");
  }
}

module.exports = {
  getMusicCategories,
  createMusicCategory,
  getMusicTracks,
  uploadMusicTrack,
  updateMusicTrack,
  streamMusicFile,
};
