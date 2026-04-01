const express = require("express");
const router = express.Router();

const {
  getSchemaAudit,
  getSchemaAuditFile,
} = require("../controllers/fundamentalsSchemaAudit.controller");
const {
  finalizeSchemaSelection,
} = require("../controllers/fundamentalsSchemaFinalize.controller");

router.get("/", getSchemaAudit);
router.get("/file", getSchemaAuditFile);
router.post("/finalize", finalizeSchemaSelection);

module.exports = router;
