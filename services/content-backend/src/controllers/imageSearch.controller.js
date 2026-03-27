const { response } = require("../utils/response.utils");
const { searchImages } = require("../services/imageSearch.service");

async function search(req, res) {
  try {
    const data = await searchImages({
      query: req.query?.query,
      provider: req.query?.provider,
      perPage: req.query?.perPage,
    });
    return response(res, 200, "Images", data);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to search images");
  }
}

module.exports = { search };
