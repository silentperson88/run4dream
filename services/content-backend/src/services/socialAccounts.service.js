const { getPool } = require("../db/newsIngest.db");
const { ensureSchema } = require("../db/socialAccounts.db");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const remotionPreviewRenderService = require("./remotionPreviewRender.service");

function trimTrailingSlash(input) {
  return String(input || "").trim().replace(/\/+$/, "");
}

function normalizePlatform(platform) {
  return String(platform || "").trim().toLowerCase();
}

async function listConnections({ userId }) {
  await ensureSchema();
  const db = getPool();
  const res = await db.query(
    `
      SELECT
        id,
        user_id,
        platform,
        account_label,
        is_connected,
        connection_data,
        notes,
        last_verified_at,
        created_at,
        updated_at
      FROM social_account_connections
      WHERE user_id = $1
      ORDER BY platform ASC
    `,
    [Number(userId || 0)],
  );
  return res.rows || [];
}

async function getConnection({ userId, platform }) {
  await ensureSchema();
  const db = getPool();
  const normalizedPlatform = normalizePlatform(platform);
  const res = await db.query(
    `
      SELECT
        id,
        user_id,
        platform,
        account_label,
        is_connected,
        connection_data,
        notes,
        last_verified_at,
        created_at,
        updated_at
      FROM social_account_connections
      WHERE user_id = $1 AND platform = $2
      LIMIT 1
    `,
    [Number(userId || 0), normalizedPlatform],
  );
  return res.rows?.[0] || null;
}

async function upsertConnection({ userId, platform, accountLabel = "", isConnected = false, connectionData = {}, notes = "" }) {
  await ensureSchema();
  const db = getPool();
  const normalizedPlatform = normalizePlatform(platform);
  const payload = connectionData && typeof connectionData === "object" ? connectionData : {};
  const res = await db.query(
    `
      INSERT INTO social_account_connections (
        user_id,
        platform,
        account_label,
        is_connected,
        connection_data,
        notes,
        last_verified_at
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6, CASE WHEN $4 THEN NOW() ELSE NULL END)
      ON CONFLICT (user_id, platform)
      DO UPDATE SET
        account_label = EXCLUDED.account_label,
        is_connected = EXCLUDED.is_connected,
        connection_data = EXCLUDED.connection_data,
        notes = EXCLUDED.notes,
        last_verified_at = CASE WHEN EXCLUDED.is_connected THEN NOW() ELSE social_account_connections.last_verified_at END,
        updated_at = NOW()
      RETURNING
        id,
        user_id,
        platform,
        account_label,
        is_connected,
        connection_data,
        notes,
        last_verified_at,
        created_at,
        updated_at
    `,
    [
      Number(userId || 0),
      normalizedPlatform,
      String(accountLabel || "").trim(),
      Boolean(isConnected),
      JSON.stringify(payload),
      String(notes || "").trim(),
    ],
  );
  return res.rows?.[0] || null;
}

async function deleteConnection({ userId, platform }) {
  await ensureSchema();
  const db = getPool();
  const normalizedPlatform = normalizePlatform(platform);
  const res = await db.query(
    `
      DELETE FROM social_account_connections
      WHERE user_id = $1 AND platform = $2
      RETURNING id
    `,
    [Number(userId || 0), normalizedPlatform],
  );
  return res.rows?.[0] || null;
}

function buildGraphApiProof(accessToken, appSecret) {
  const token = String(accessToken || "").trim();
  const secret = String(appSecret || "").trim();
  if (!token || !secret) return "";
  return crypto.createHmac("sha256", secret).update(token).digest("hex");
}

async function publishToFacebook({ userId, title, caption, imageUrl }) {
  await ensureSchema();
  const connection = await getConnection({ userId, platform: "facebook" });
  if (!connection) throw new Error("Facebook account is not linked");

  const connectionData = connection.connection_data && typeof connection.connection_data === "object"
    ? connection.connection_data
    : {};
  const pageId = String(connectionData.facebookPageId || connectionData.pageId || "").trim();
  const accessToken = String(connectionData.pageAccessToken || connectionData.accessToken || "").trim();
  const appSecret = String(connectionData.metaAppSecret || connectionData.appSecret || "").trim();
  const appId = String(connectionData.metaAppId || connectionData.appId || "").trim();
  const message = [String(title || "").trim(), String(caption || "").trim()].filter(Boolean).join("\n\n").trim();

  if (!pageId) throw new Error("Facebook Page ID is missing in the linked account");
  if (!accessToken) throw new Error("Facebook access token is missing in the linked account");

  const proof = buildGraphApiProof(accessToken, appSecret);
  const params = new URLSearchParams();
  if (message) params.set("message", message);
  if (appId && appSecret) {
    params.set("app_id", appId);
    params.set("appsecret_proof", proof);
  }
  params.set("access_token", accessToken);

  const normalizedImageUrl = String(imageUrl || "").trim();
  const usePhotoEndpoint = Boolean(normalizedImageUrl);

  let endpoint = `https://graph.facebook.com/v20.0/${encodeURIComponent(pageId)}/feed`;
  let publishResponse;
  if (usePhotoEndpoint) {
    endpoint = `https://graph.facebook.com/v20.0/${encodeURIComponent(pageId)}/photos`;
    const resolvedImageUrl = /^https?:\/\//i.test(normalizedImageUrl)
      ? normalizedImageUrl
      : `${trimTrailingSlash(process.env.USER_PUBLIC_BASE_URL || "")}/${normalizedImageUrl.replace(/^\/+/, "")}`;
    const imageResponse = await fetch(resolvedImageUrl);
    if (!imageResponse.ok) {
      throw new Error(`Unable to fetch image for Facebook publish (${imageResponse.status})`);
    }
    const contentType = imageResponse.headers.get("content-type") || "image/jpeg";
    const arrayBuffer = await imageResponse.arrayBuffer();
    const sourceBlob = new Blob([arrayBuffer], { type: contentType });
    const form = new FormData();
    if (message) form.append("caption", message);
    form.append("access_token", accessToken);
    if (appId && appSecret) {
      form.append("app_id", appId);
      form.append("appsecret_proof", proof);
    }
    form.append("published", "true");
    form.append("source", sourceBlob, "facebook-post-image");
    publishResponse = await fetch(endpoint, {
      method: "POST",
      body: form,
    });
  } else {
    publishResponse = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: params.toString(),
    });
  }

  const responseText = await publishResponse.text();
  let payload = null;
  try {
    payload = responseText ? JSON.parse(responseText) : null;
  } catch {
    payload = { raw: responseText };
  }

  if (!publishResponse.ok) {
    const errorMessage =
      payload?.error?.message ||
      payload?.message ||
      responseText ||
      `Facebook publish failed (${publishResponse.status})`;
    const err = new Error(errorMessage);
    err.statusCode = publishResponse.status;
    err.payload = payload;
    throw err;
  }

  return {
    platform: "facebook",
    pageId,
    accessTokenPresent: Boolean(accessToken),
    publishedWith: usePhotoEndpoint ? "photo" : "feed",
    usedLinkedAccount: true,
    accountWasMarkedActive: Boolean(connection.is_connected),
    result: payload,
    connection: {
      id: connection.id,
      accountLabel: connection.account_label,
      isConnected: connection.is_connected,
    },
  };
}

function toAbsoluteMediaUrl(input) {
  const raw = String(input || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw) || raw.startsWith("data:")) return raw;
  const base = trimTrailingSlash(process.env.USER_PUBLIC_BASE_URL || "");
  if (!base) return raw;
  return `${base}/${raw.replace(/^\/+/, "")}`;
}

async function waitForRemotionJob(jobId, timeoutMs = 10 * 60 * 1000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const job = remotionPreviewRenderService.getRemotionPreviewJob(jobId);
    if (job.status === "completed") return job;
    if (job.status === "failed") throw new Error(job.error || "Remotion render failed");
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  throw new Error("Timed out while rendering social video");
}

async function prepareFacebookTemplateVideo({ userId, title, caption, templateProps }) {
  const connection = await getConnection({ userId, platform: "facebook" });
  if (!connection) throw new Error("Facebook account is not linked");

  const draftProps = templateProps && typeof templateProps === "object" ? templateProps : {};
  const socialVideoProps = {
    ...draftProps,
    brand: String(draftProps.brand || "Run4Dream"),
    title: String(title || draftProps.title || "Run4Dream"),
    subtitle: String(draftProps.subtitle || draftProps.shortText || draftProps.mediumText || caption || ""),
    shortText: String(draftProps.shortText || caption || ""),
    mediumText: String(draftProps.mediumText || caption || ""),
    highlights: Array.isArray(draftProps.highlights) ? draftProps.highlights : [],
    cta: String(draftProps.cta || ""),
    image: toAbsoluteMediaUrl(draftProps.image || ""),
    audioUrl: toAbsoluteMediaUrl(draftProps.audioUrl || ""),
    imageMode: draftProps.imageMode === "original" ? "original" : "custom",
  };
  const durationSec = Math.max(1, Number(draftProps.durationSec || 10));
  const durationInFrames = Math.max(1, Math.round(durationSec * 30));

  if (!socialVideoProps.image) {
    throw new Error("Template 1 needs an image before preparing for Facebook");
  }
  if (!socialVideoProps.audioUrl) {
    throw new Error("Template 1 needs linked music before preparing for Facebook");
  }

  const job = await remotionPreviewRenderService.createRemotionPreviewJob({
    props: socialVideoProps,
    compositionId: "NewsSocialTemplateOne",
    durationInFrames,
    renderFrameEnd: durationInFrames - 1,
    qualityMode: "high",
    useGpu: false,
  });

  return {
    jobId: job.jobId,
    fileName: job.fileName,
    estimatedRenderSeconds: job.estimatedRenderSeconds,
    renderStatus: "queued",
    readyToPublish: false,
    accountLabel: connection.account_label,
  };
}

function getFacebookTemplateRenderStatus(jobId) {
  const job = remotionPreviewRenderService.getRemotionPreviewJob(jobId);
  return {
    ...job,
    readyToPublish: job.status === "completed",
  };
}

async function publishFacebookTemplateVideo({ userId, title, caption, templateProps, renderJobId }) {
  const connection = await getConnection({ userId, platform: "facebook" });
  if (!connection) throw new Error("Facebook account is not linked");

  const connectionData =
    connection.connection_data && typeof connection.connection_data === "object"
      ? connection.connection_data
      : {};
  const pageId = String(connectionData.facebookPageId || connectionData.pageId || "").trim();
  const accessToken = String(connectionData.pageAccessToken || connectionData.accessToken || "").trim();
  const appSecret = String(connectionData.metaAppSecret || connectionData.appSecret || "").trim();
  const appId = String(connectionData.metaAppId || connectionData.appId || "").trim();
  if (!pageId) throw new Error("Facebook Page ID is missing in the linked account");
  if (!accessToken) throw new Error("Facebook access token is missing in the linked account");

  let job = null;
  let videoPath = "";

  if (renderJobId) {
    const preparedJob = remotionPreviewRenderService.getRemotionPreviewJob(renderJobId);
    if (preparedJob.status !== "completed") {
      throw new Error("Facebook render is not ready yet");
    }
    job = preparedJob;
    videoPath = String(preparedJob.filePath || "");
  } else {
    const draftProps = templateProps && typeof templateProps === "object" ? templateProps : {};
    const socialVideoProps = {
      ...draftProps,
      brand: String(draftProps.brand || "Run4Dream"),
      title: String(title || draftProps.title || "Run4Dream"),
      subtitle: String(draftProps.subtitle || draftProps.shortText || draftProps.mediumText || caption || ""),
      shortText: String(draftProps.shortText || caption || ""),
      mediumText: String(draftProps.mediumText || caption || ""),
      highlights: Array.isArray(draftProps.highlights) ? draftProps.highlights : [],
      cta: String(draftProps.cta || ""),
      image: toAbsoluteMediaUrl(draftProps.image || ""),
      audioUrl: toAbsoluteMediaUrl(draftProps.audioUrl || ""),
      imageMode: draftProps.imageMode === "original" ? "original" : "custom",
    };
    const durationSec = Math.max(1, Number(draftProps.durationSec || 10));
    const durationInFrames = Math.max(1, Math.round(durationSec * 30));

    if (!socialVideoProps.image) {
      throw new Error("Template 1 needs an image before publishing to Facebook");
    }
    if (!socialVideoProps.audioUrl) {
      throw new Error("Template 1 needs linked music before publishing to Facebook");
    }

    job = await remotionPreviewRenderService.createRemotionPreviewJob({
      props: socialVideoProps,
      compositionId: "NewsSocialTemplateOne",
      durationInFrames,
      renderFrameEnd: durationInFrames - 1,
      qualityMode: "high",
      useGpu: false,
    });
    const finishedJob = await waitForRemotionJob(job.jobId);
    videoPath = String(finishedJob.filePath || "");
  }

  if (!videoPath || !fs.existsSync(videoPath)) {
    throw new Error("Rendered Facebook video not found");
  }

  const videoBuffer = fs.readFileSync(videoPath);
  const videoBlob = new Blob([videoBuffer], { type: "video/mp4" });
  const form = new FormData();
  form.append("access_token", accessToken);
  form.append("source", videoBlob, `${String(job?.fileName || path.basename(videoPath) || "facebook-post.mp4")}`);
  form.append(
    "description",
    [String(title || "").trim(), String(caption || "").trim()].filter(Boolean).join("\n\n"),
  );
  if (appId && appSecret) {
    form.append("app_id", appId);
    form.append("appsecret_proof", buildGraphApiProof(accessToken, appSecret));
  }

  const publishResponse = await fetch(`https://graph.facebook.com/v20.0/${encodeURIComponent(pageId)}/videos`, {
    method: "POST",
    body: form,
  });
  const responseText = await publishResponse.text();
  let payload = null;
  try {
    payload = responseText ? JSON.parse(responseText) : null;
  } catch {
    payload = { raw: responseText };
  }

  if (!publishResponse.ok) {
    const errorMessage =
      payload?.error?.message ||
      payload?.message ||
      responseText ||
      `Facebook publish failed (${publishResponse.status})`;
    const err = new Error(errorMessage);
    err.statusCode = publishResponse.status;
    err.payload = payload;
    throw err;
  }

  return {
    platform: "facebook",
    pageId,
    accessTokenPresent: Boolean(accessToken),
    publishedWith: "video",
    usedLinkedAccount: true,
    accountWasMarkedActive: Boolean(connection.is_connected),
    renderJobId: job?.jobId || renderJobId || "",
    renderFileName: job?.fileName || path.basename(videoPath),
    result: payload,
  };
}

module.exports = {
  listConnections,
  getConnection,
  upsertConnection,
  deleteConnection,
  normalizePlatform,
  publishToFacebook,
  prepareFacebookTemplateVideo,
  getFacebookTemplateRenderStatus,
  publishFacebookTemplateVideo,
};
