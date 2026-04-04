const fundamentalsRepo = require("../repositories/fundamentals.repository");

exports.createEntry = async (masterStockId, activeStockId, db) =>
  fundamentalsRepo.createEntry(masterStockId, activeStockId, db);

exports.linkActiveStockId = async (masterStockId, activeStockId, db) =>
  fundamentalsRepo.linkActiveStockId(masterStockId, activeStockId, db);

exports.updateEntry = async (data) => {
  const entry = await fundamentalsRepo.updateLegacyEntry(data);
  if (!entry) {
    throw new Error("DOCUMENT_NOT_FOUND");
  }
  return entry;
};

exports.upsertFundamentals = async (data, db) =>
  fundamentalsRepo.upsertByMasterId(data, db);

exports.getFullStockFundamentals = async (masterStockId) =>
  fundamentalsRepo.getByMasterId(masterStockId);

exports.listMasterFreshness = async () => fundamentalsRepo.listMasterFreshness();
