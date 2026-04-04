require("dotenv").config();

const { dbReady, pool } = require("../config/db");
const {
  loadAudit,
  buildFinalSchemaFromSelection,
  saveFinalSchema,
  saveFinalSchemaSql,
} = require("../services/fundamentalsSchemaAudit.service");

const ensureQualityColumns = async (db = pool) => {
  await db.query(`
    ALTER TABLE stock_master
      ADD COLUMN IF NOT EXISTS fundamentals_checked_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS fundamentals_failed_fields TEXT[],
      ADD COLUMN IF NOT EXISTS fundamentals_failed_reason TEXT
  `);
};

const loadFinalSchemaSql = async () => {
  const audit = await loadAudit();
  const schema = buildFinalSchemaFromSelection(audit, {});
  await saveFinalSchema(schema);
  const { sql } = await saveFinalSchemaSql(schema);
  return sql;
};

const splitStatements = (sql) =>
  String(sql)
    .split(/\r?\n/)
    .filter((line) => !line.trim().startsWith("--"))
    .join("\n")
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean)
    .filter((statement) => {
      const upper = statement.toUpperCase();
      return (
        upper.includes("CREATE TABLE") ||
        upper.includes("ALTER TABLE") ||
        upper.includes("DROP TABLE")
      );
    });

const run = async () => {
  console.log("Starting fundamentals final-schema apply...");
  await dbReady;

  const sql = await loadFinalSchemaSql();
  const statements = splitStatements(sql);

  console.log(`Applying ${statements.length} SQL statements from final schema`);

  await ensureQualityColumns(pool);

  for (let i = 0; i < statements.length; i += 1) {
    const statement = statements[i];
    // eslint-disable-next-line no-await-in-loop
    await pool.query(statement);
    console.log(`[${i + 1}/${statements.length}] Applied statement`);
  }

  console.log("Final schema apply completed successfully.");
};

run()
  .catch((error) => {
    console.error("Final schema apply failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch (error) {
      console.error("Pool shutdown warning:", error?.message || error);
    }
  });
