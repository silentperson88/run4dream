const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");
const {
  loadAudit,
  buildFinalSchemaFromSelection,
  saveFinalSchema,
  saveFinalSchemaSql,
  FINAL_SCHEMA_FILE_PATH,
  FINAL_SCHEMA_SQL_FILE_PATH,
} = require("../services/fundamentalsSchemaAudit.service");

exports.finalizeSchemaSelection = async (req, res) => {
  try {
    const selection = req.body?.selected_tables || req.body?.selection || {};
    const audit = await loadAudit();
    const finalSchema = buildFinalSchemaFromSelection(audit, selection);
    const filePath = await saveFinalSchema(finalSchema);
    const sqlArtifact = await saveFinalSchemaSql(finalSchema);

    return response(res, 200, responseUtils.SUCCESS, {
      file_path: filePath,
      sql_file_path: sqlArtifact?.filePath || FINAL_SCHEMA_SQL_FILE_PATH,
      sql: sqlArtifact?.sql || null,
      data: finalSchema,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error?.message || "Failed to finalize schema selection",
      file_path: FINAL_SCHEMA_FILE_PATH,
      sql_file_path: FINAL_SCHEMA_SQL_FILE_PATH,
    });
  }
};

exports.getFinalSchema = async (_req, res) => {
  try {
    const audit = await loadAudit();
    const finalSchema = buildFinalSchemaFromSelection(audit, {});
    return response(res, 200, responseUtils.SUCCESS, {
      file_path: FINAL_SCHEMA_FILE_PATH,
      data: finalSchema,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error?.message || "Failed to build final schema",
    });
  }
};
