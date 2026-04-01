const fs = require("fs/promises");
const { response } = require("../utils/response.utils");
const { responseUtils } = require("../utils/Constants/responseContants.utils");
const {
  AUDIT_FILE_PATH,
  loadAudit,
} = require("../services/fundamentalsSchemaAudit.service");

exports.getSchemaAudit = async (_req, res) => {
  try {
    const audit = await loadAudit();
    return response(res, 200, responseUtils.SUCCESS, {
      file_path: AUDIT_FILE_PATH,
      data: audit,
    });
  } catch (error) {
    return response(res, 500, responseUtils.SERVER_ERROR, {
      message: error?.message || "Failed to load schema audit",
    });
  }
};

exports.getSchemaAuditFile = async (_req, res) => {
  try {
    const raw = await fs.readFile(AUDIT_FILE_PATH, "utf8");
    return res.type("json").send(raw);
  } catch (error) {
    return response(res, 404, responseUtils.STOCK_NOT_FOUND, {
      message: "Schema audit file not found. Run the audit script first.",
      file_path: AUDIT_FILE_PATH,
    });
  }
};
