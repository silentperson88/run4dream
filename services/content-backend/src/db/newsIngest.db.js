const { Pool } = require("pg");

let pool = null;
let schemaInitPromise = null;

function resolveDbUrl() {
  return process.env.CONTENT_DB_URL || process.env.DATABASE_URL || "";
}

function getPool() {
  if (pool) return pool;
  const connectionString = resolveDbUrl();
  if (!connectionString) {
    throw new Error("CONTENT_DB_URL is required for BSE ingestion APIs");
  }
  pool = new Pool({ connectionString });
  return pool;
}

async function ensureSchema() {
  if (schemaInitPromise) return schemaInitPromise;
  schemaInitPromise = (async () => {
    const db = getPool();
    await db.query(`
      DROP TABLE IF EXISTS bse_ingest_items;
    `);
    await db.query(`
      DROP TABLE IF EXISTS bse_ingest_runs;
    `);
    await db.query(
      `
        CREATE TABLE IF NOT EXISTS bse_news (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          news_id TEXT NOT NULL UNIQUE,
          news_date TEXT,
          disseminated_at TEXT,
          scrip_code TEXT,
          company TEXT,
          headline TEXT,
          category TEXT,
          announcement_type TEXT,
          pdf_url TEXT,
          match_status TEXT NOT NULL DEFAULT 'pending',
          match_source TEXT,
          match_score INTEGER NOT NULL DEFAULT 0,
          metadata_score INTEGER NOT NULL DEFAULT 0,
          pdf_score INTEGER NOT NULL DEFAULT 0,
          matched_themes TEXT[] NOT NULL DEFAULT '{}',
          matched_terms JSONB NOT NULL DEFAULT '{}'::jsonb,
          pdf_text TEXT,
          raw_data JSONB NOT NULL,
          fetched_for_date DATE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS match_source TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS metadata_score INTEGER NOT NULL DEFAULT 0;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS pdf_score INTEGER NOT NULL DEFAULT 0;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS pdf_text TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS important_points_text TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS script_english TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS script_hindi TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS script_audio_english TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS script_audio_hindi TEXT;");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS highlight_terms TEXT[] NOT NULL DEFAULT '{}';");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS highlight_terms_positive TEXT[] NOT NULL DEFAULT '{}';");
    await db.query("ALTER TABLE bse_news ADD COLUMN IF NOT EXISTS highlight_terms_negative TEXT[] NOT NULL DEFAULT '{}';");
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_news_created_at ON bse_news(created_at DESC);",
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_news_match_status ON bse_news(match_status);",
    );
    await db.query(
      `
        CREATE TABLE IF NOT EXISTS bse_news_categories (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          category_name TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (user_id, category_name)
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_news_categories_user_id ON bse_news_categories(user_id);",
    );
    await db.query(
      `
        CREATE TABLE IF NOT EXISTS news_approach_jobs (
          id TEXT PRIMARY KEY,
          user_id BIGINT NOT NULL,
          status TEXT NOT NULL,
          date DATE NOT NULL,
          category TEXT NOT NULL DEFAULT 'all',
          forced_match_status TEXT NOT NULL DEFAULT 'matched',
          model TEXT,
          total INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          success INTEGER NOT NULL DEFAULT 0,
          failed INTEGER NOT NULL DEFAULT 0,
          skipped INTEGER NOT NULL DEFAULT 0,
          current_news_id BIGINT,
          current_headline TEXT,
          gap_ms INTEGER NOT NULL DEFAULT 0,
          cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
          errors JSONB NOT NULL DEFAULT '[]'::jsonb,
          started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_news_approach_jobs_user_status ON news_approach_jobs(user_id, status);",
    );
    await db.query(
      `
        CREATE TABLE IF NOT EXISTS bse_full_videos (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          video_date DATE NOT NULL,
          scope_category TEXT NOT NULL DEFAULT 'all',
          title TEXT,
          render_job_id TEXT,
          file_name TEXT,
          video_url TEXT,
          status TEXT NOT NULL DEFAULT 'completed',
          total_news INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_full_videos_user_date ON bse_full_videos(user_id, video_date DESC, created_at DESC);",
    );
    await db.query(
      "CREATE UNIQUE INDEX IF NOT EXISTS uq_bse_full_videos_user_render_job ON bse_full_videos(user_id, render_job_id) WHERE render_job_id IS NOT NULL;",
    );
    await db.query(
      `
        CREATE TABLE IF NOT EXISTS bse_full_video_news (
          id BIGSERIAL PRIMARY KEY,
          video_id BIGINT NOT NULL REFERENCES bse_full_videos(id) ON DELETE CASCADE,
          user_id BIGINT NOT NULL,
          news_row_id BIGINT NOT NULL,
          news_id TEXT,
          company TEXT,
          headline TEXT,
          category TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_full_video_news_user_news_row ON bse_full_video_news(user_id, news_row_id);",
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_bse_full_video_news_video_id ON bse_full_video_news(video_id);",
    );

    await db.query(
      `
        CREATE TABLE IF NOT EXISTS news_content_videos (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          language TEXT NOT NULL DEFAULT 'english',
          script TEXT NOT NULL DEFAULT '',
          clip_approach TEXT NOT NULL DEFAULT 'multi_sentence',
          sentence_keywords JSONB NOT NULL DEFAULT '{}'::jsonb,
          audio_url TEXT,
          clips JSONB NOT NULL DEFAULT '[]'::jsonb,
          status TEXT NOT NULL DEFAULT 'draft',
          render_job_id TEXT,
          file_name TEXT,
          video_url TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_news_content_videos_user_id ON news_content_videos(user_id, created_at DESC);",
    );
    await db.query("ALTER TABLE news_content_videos ADD COLUMN IF NOT EXISTS render_job_id TEXT;");
    await db.query("ALTER TABLE news_content_videos ADD COLUMN IF NOT EXISTS file_name TEXT;");
    await db.query("ALTER TABLE news_content_videos ADD COLUMN IF NOT EXISTS video_url TEXT;");
    await db.query("ALTER TABLE news_content_videos ADD COLUMN IF NOT EXISTS sentence_keywords JSONB NOT NULL DEFAULT '{}'::jsonb;");
    await db.query("ALTER TABLE news_content_videos ADD COLUMN IF NOT EXISTS clip_approach TEXT NOT NULL DEFAULT 'multi_sentence';");

    await db.query(
      `
        CREATE TABLE IF NOT EXISTS audio_tools_presets (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          preset_name TEXT NOT NULL,
          preset_config JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (user_id, preset_name)
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_audio_tools_presets_user_id ON audio_tools_presets(user_id, created_at DESC);",
    );

    await db.query(
      `
        CREATE TABLE IF NOT EXISTS news_rss_items (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          source TEXT NOT NULL,
          title TEXT NOT NULL,
          link TEXT NOT NULL UNIQUE,
          pub_date TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          raw_text TEXT,
          cleaned_text TEXT,
          images JSONB NOT NULL DEFAULT '[]'::jsonb,
          template_one JSONB,
          template_two JSONB,
          template_three JSONB,
          template_generated_at TIMESTAMPTZ,
          news_content_video_id BIGINT,
          error TEXT,
          attempts INTEGER NOT NULL DEFAULT 0,
          started_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `,
    );
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_news_rss_items_user_id ON news_rss_items(user_id, created_at DESC);",
    );
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS raw_text TEXT;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS cleaned_text TEXT;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS images JSONB NOT NULL DEFAULT '[]'::jsonb;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS template_one JSONB;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS template_two JSONB;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS template_three JSONB;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS template_generated_at TIMESTAMPTZ;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS news_content_video_id BIGINT;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS error TEXT;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;");
    await db.query("ALTER TABLE news_rss_items ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;");
    await db.query(
      "CREATE INDEX IF NOT EXISTS idx_news_rss_items_status ON news_rss_items(status, created_at DESC);",
    );
  })();
  return schemaInitPromise;
}

module.exports = {
  getPool,
  ensureSchema,
};
