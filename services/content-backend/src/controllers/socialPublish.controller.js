const { response } = require("../utils/response.utils");
const { updateRssItemDraft, getRssById } = require("../services/rssSaved.service");
const {
  normalizePlatform,
  publishToFacebook,
  publishFacebookTemplateVideo,
  prepareFacebookTemplateVideo,
  getFacebookTemplateRenderStatus,
} = require("../services/socialAccounts.service");

async function publishSocialPost(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    const title = String(req.body?.title || "").trim();
    const caption = String(req.body?.caption || "").trim();
    const imageUrl = String(req.body?.imageUrl || "").trim();
    const templateType = String(req.body?.templateType || "").trim();
    const rssItemId = Number(req.body?.rssItemId || 0);
    const templateProps = req.body?.templateProps && typeof req.body.templateProps === "object"
      ? req.body.templateProps
      : null;
    const renderJobId = String(req.body?.renderJobId || "").trim();

    if (!platform) return response(res, 400, "platform is required");

    if (rssItemId > 0) {
      const existing = await getRssById({ id: rssItemId });
      const state = existing?.platform_post_state && typeof existing.platform_post_state === "object"
        ? existing.platform_post_state
        : {};
      const scheduleState = existing?.platform_schedule_state && typeof existing.platform_schedule_state === "object"
        ? existing.platform_schedule_state
        : {};
      if (scheduleState?.[platform]?.scheduled && !state?.[platform]?.published) {
        return response(res, 409, `${platform} is already scheduled for this item`, scheduleState[platform]);
      }
      if (state?.[platform]?.published) {
        return response(res, 409, `${platform} has already been posted for this item`, state[platform]);
      }
    }

    if (platform === "facebook") {
      if (templateType === "templateOne" && templateProps) {
        const result = await publishFacebookTemplateVideo({
          userId,
          title,
          caption,
          templateProps,
          renderJobId,
        });
        if (rssItemId > 0) {
          const existing = await getRssById({ id: rssItemId });
          const currentState = existing?.platform_post_state && typeof existing.platform_post_state === "object"
            ? existing.platform_post_state
            : {};
          const nextState = {
            ...currentState,
            [platform]: {
              published: true,
              templateType,
              publishedAt: new Date().toISOString(),
              title,
              caption,
              renderJobId: result.renderJobId || "",
              renderFileName: result.renderFileName || "",
              postId: result?.result?.id || result?.result?.post_id || "",
              platform,
            },
          };
          await updateRssItemDraft({
            id: rssItemId,
            platformPostState: nextState,
          });
        }
        return response(res, 200, "Facebook template video published", result);
      }
      const result = await publishToFacebook({ userId, title, caption, imageUrl });
      if (rssItemId > 0) {
        const existing = await getRssById({ id: rssItemId });
        const currentState = existing?.platform_post_state && typeof existing.platform_post_state === "object"
          ? existing.platform_post_state
          : {};
        const nextState = {
          ...currentState,
          [platform]: {
            published: true,
            templateType,
            publishedAt: new Date().toISOString(),
            title,
            caption,
            postId: result?.result?.id || result?.result?.post_id || "",
            platform,
          },
        };
        await updateRssItemDraft({
          id: rssItemId,
          platformPostState: nextState,
        });
      }
      return response(res, 200, "Facebook post published", result);
    }

    return response(res, 501, `${platform} publishing is not wired yet`);
  } catch (err) {
    return response(res, err?.statusCode || 400, err?.message || "Unable to publish social post", err?.payload || {});
  }
}

async function prepareSocialPost(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    const title = String(req.body?.title || "").trim();
    const caption = String(req.body?.caption || "").trim();
    const templateType = String(req.body?.templateType || "").trim();
    const rssItemId = Number(req.body?.rssItemId || 0);
    const templateProps = req.body?.templateProps && typeof req.body.templateProps === "object"
      ? req.body.templateProps
      : null;
    if (!platform) return response(res, 400, "platform is required");

    if (platform === "facebook" && templateType === "templateOne" && templateProps) {
      const job = await prepareFacebookTemplateVideo({
        userId,
        title,
        caption,
        templateProps,
      });
      if (rssItemId > 0) {
        await updateRssItemDraft({
          id: rssItemId,
          previewRenderState: {
            platform: "facebook",
            templateType,
            jobId: job.jobId,
            fileName: job.fileName,
            status: job.renderStatus || "queued",
            message: "Facebook render started",
            updatedAt: new Date().toISOString(),
          },
        });
      }
      return response(res, 200, "Facebook template is ready to render", job);
    }

    return response(res, 501, `${platform} render prep is not wired yet`);
  } catch (err) {
    return response(res, err?.statusCode || 400, err?.message || "Unable to prepare social post", err?.payload || {});
  }
}

async function getPrepareSocialPostStatus(req, res) {
  try {
    const platform = normalizePlatform(req.params?.platform);
    const jobId = String(req.params?.jobId || "");
    if (!platform) return response(res, 400, "platform is required");

    if (platform === "facebook") {
      const job = getFacebookTemplateRenderStatus(jobId);
      return response(res, 200, "Facebook render status", job);
    }

    return response(res, 501, `${platform} render status is not wired yet`);
  } catch (err) {
    return response(res, 404, err?.message || "Render job not found");
  }
}

async function scheduleSocialPost(req, res) {
  try {
    const userId = Number(req.user?.id || 0);
    const platform = normalizePlatform(req.params?.platform);
    const rssItemId = Number(req.body?.rssItemId || 0);
    const title = String(req.body?.title || "").trim();
    const caption = String(req.body?.caption || "").trim();
    const scheduledAt = String(req.body?.scheduledAt || "").trim();
    const templateType = String(req.body?.templateType || "").trim();
    const templateProps = req.body?.templateProps && typeof req.body.templateProps === "object"
      ? req.body.templateProps
      : null;
    const renderJobId = String(req.body?.renderJobId || "").trim();

    if (!platform) return response(res, 400, "platform is required");
    if (!rssItemId) return response(res, 400, "rssItemId is required");
    if (!scheduledAt) return response(res, 400, "scheduledAt is required");

    const when = new Date(scheduledAt);
    if (Number.isNaN(when.getTime())) {
      return response(res, 400, "scheduledAt is invalid");
    }

    const existing = await getRssById({ id: rssItemId });
    if (!existing) return response(res, 404, "RSS item not found");
    const postState = existing?.platform_post_state && typeof existing.platform_post_state === "object"
      ? existing.platform_post_state
      : {};
    if (postState?.[platform]?.published) {
      return response(res, 409, `${platform} has already been posted for this item`, postState[platform]);
    }

    const scheduleState = existing?.platform_schedule_state && typeof existing.platform_schedule_state === "object"
      ? existing.platform_schedule_state
      : {};
    const nextSchedule = {
      ...scheduleState,
      [platform]: {
        platform,
        scheduled: true,
        scheduledAt: when.toISOString(),
        templateType,
        title,
        caption,
        renderJobId,
        templateProps,
        status: "scheduled",
        updatedAt: new Date().toISOString(),
        scheduledBy: userId,
      },
    };
    const updated = await updateRssItemDraft({
      id: rssItemId,
      platformScheduleState: nextSchedule,
    });
    return response(res, 200, "Social post scheduled", {
      ...(nextSchedule[platform] || {}),
      row: updated,
    });
  } catch (err) {
    return response(res, err?.statusCode || 400, err?.message || "Unable to schedule social post", err?.payload || {});
  }
}

module.exports = {
  publishSocialPost,
  prepareSocialPost,
  getPrepareSocialPostStatus,
  scheduleSocialPost,
};
