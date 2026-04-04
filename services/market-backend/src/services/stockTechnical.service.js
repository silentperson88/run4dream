const stockTechnicalRepository = require("../repositories/stockTechnical.repository");

exports.getMomentumSnapshotByMasterId = async (masterId) =>
  stockTechnicalRepository.getMomentumSnapshotByMasterId(masterId);
