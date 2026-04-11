const { listRssItemsWithSchedules, updateRssItemDraft } = require("./rssSaved.service");
const { publishToFacebook, publishFacebookTemplateVideo, normalizePlatform } = require("./socialAccounts.service");

let timer = null;
let running = false;

function normalizeScheduleMap(state) {
  return state && typeof state === "object" ? state : {};
}

async function processDueSchedules() {
  if (running) return;
  running = true;
  try {
    const rows = await listRssItemsWithSchedules();
    const now = Date.now();
    for (const row of rows) {
      const scheduleMap = normalizeScheduleMap(row.platform_schedule_state);
      const postMap = normalizeScheduleMap(row.platform_post_state);
      let changed = false;
      let nextScheduleMap = { ...scheduleMap };
      let nextPostMap = { ...postMap };

      for (const [platformKey, entry] of Object.entries(scheduleMap)) {
        const platform = normalizePlatform(platformKey);
        if (!entry || String(entry.status || "").toLowerCase() !== "scheduled") continue;
        const scheduledAt = new Date(entry.scheduledAt || entry.scheduled_at || 0).getTime();
        if (!scheduledAt || scheduledAt > now) continue;
        if (nextPostMap?.[platform]?.published) {
          nextScheduleMap[platform] = {
            ...entry,
            status: "published",
            message: "Already published for this item",
            updatedAt: new Date().toISOString(),
          };
          changed = true;
          continue;
        }

        if (platform !== "facebook") {
          nextScheduleMap[platform] = {
            ...entry,
            status: "blocked",
            message: `${platform} publishing is not wired yet`,
            updatedAt: new Date().toISOString(),
          };
          changed = true;
          continue;
        }

        try {
          const result =
            entry.templateType === "templateOne" && entry.templateProps
              ? await publishFacebookTemplateVideo({
                  userId: Number(row.user_id || 0),
                  title: String(entry.title || row.title || ""),
                  caption: String(entry.caption || ""),
                  templateProps: entry.templateProps,
                  link: String(row.link || ""),
                  renderJobId: String(entry.renderJobId || ""),
                })
              : await publishToFacebook({
                  userId: Number(row.user_id || 0),
                  title: String(entry.title || row.title || ""),
                  caption: String(entry.caption || ""),
                  imageUrl: String(entry.imageUrl || row.images?.[0] || ""),
                  link: String(row.link || ""),
                });

          nextScheduleMap[platform] = {
            ...entry,
            status: "published",
            publishedAt: new Date().toISOString(),
            message: "Published successfully",
            updatedAt: new Date().toISOString(),
            result: result?.result || null,
          };
          nextPostMap[platform] = {
            published: true,
            templateType: entry.templateType || "",
            publishedAt: new Date().toISOString(),
            title: String(entry.title || row.title || ""),
            caption: String(entry.caption || ""),
            renderJobId: result?.renderJobId || entry.renderJobId || "",
            renderFileName: result?.renderFileName || "",
            postId: result?.result?.id || result?.result?.post_id || "",
            platform,
          };
          changed = true;
        } catch (err) {
          nextScheduleMap[platform] = {
            ...entry,
            status: "failed",
            message: err?.message || "Scheduled publish failed",
            updatedAt: new Date().toISOString(),
          };
          changed = true;
        }
      }

      if (changed) {
        await updateRssItemDraft({
          id: row.id,
          platformScheduleState: nextScheduleMap,
          platformPostState: nextPostMap,
        });
      }
    }
  } catch (err) {
    console.error("Social schedule runner failed:", err);
  } finally {
    running = false;
  }
}

function startSocialScheduleRunner() {
  if (timer) return;
  void processDueSchedules();
  timer = setInterval(() => {
    void processDueSchedules();
  }, 30_000);
}

function stopSocialScheduleRunner() {
  if (timer) clearInterval(timer);
  timer = null;
}

module.exports = {
  startSocialScheduleRunner,
  stopSocialScheduleRunner,
  processDueSchedules,
};
