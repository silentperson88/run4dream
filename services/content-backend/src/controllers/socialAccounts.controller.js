const path = require("path");
const { response } = require("../utils/response.utils");
const {
  listConnections,
  getConnection,
  upsertConnection,
  deleteConnection,
  normalizePlatform,
} = require("../services/socialAccounts.service");

const socialAccountSchema = require(path.join(__dirname, "..", "config", "social-account-schema.json"));

async function getSocialAccountSchema(req, res) {
  return response(res, 200, "Social account schema", socialAccountSchema);
}

async function getSocialAccounts(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const accounts = await listConnections({ userId });
    return response(res, 200, "Social accounts", accounts);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to load social accounts");
  }
}

async function getSocialAccount(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    if (!platform) return response(res, 400, "platform is required");
    const account = await getConnection({ userId, platform });
    return response(res, 200, "Social account", account);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to load social account");
  }
}

async function upsertSocialAccount(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    const accountLabel = String(req.body?.accountLabel || "").trim();
    const isConnected = req.body?.isConnected === true || String(req.body?.isConnected || "").toLowerCase() === "true";
    const notes = String(req.body?.notes || "").trim();
    const connectionData = req.body?.connectionData && typeof req.body.connectionData === "object"
      ? req.body.connectionData
      : {};
    if (!platform) return response(res, 400, "platform is required");
    const saved = await upsertConnection({
      userId,
      platform,
      accountLabel,
      isConnected,
      connectionData,
      notes,
    });
    return response(res, 200, "Social account saved", saved);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to save social account");
  }
}

async function deleteSocialAccount(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    if (!platform) return response(res, 400, "platform is required");
    const deleted = await deleteConnection({ userId, platform });
    if (!deleted) return response(res, 404, "Social account not found");
    return response(res, 200, "Social account deleted", deleted);
  } catch (err) {
    return response(res, 400, err?.message || "Unable to delete social account");
  }
}

module.exports = {
  getSocialAccountSchema,
  getSocialAccounts,
  getSocialAccount,
  upsertSocialAccount,
  deleteSocialAccount,
};
