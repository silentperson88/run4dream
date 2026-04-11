const fundamentalsRepo = require("../repositories/fundamentals.repository");
const fundamentalsOverviewRepo = require("../repositories/fundamentalsOverview.repository");
const fundamentalsQuarterlyRepo = require("../repositories/fundamentalsQuarterly.repository");
const fundamentalsSplitRepo = require("../repositories/fundamentalsSplit.repository");

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

exports.getOverviewFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsOverviewRepo.getByMasterId(masterStockId);

exports.getOverviewFundamentalsBySymbol = async (symbol) =>
  fundamentalsOverviewRepo.getBySymbol(symbol);

exports.getQuarterlyFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsQuarterlyRepo.getByMasterId(masterStockId);

exports.getQuarterlyFundamentalsBySymbol = async (symbol) =>
  fundamentalsQuarterlyRepo.getBySymbol(symbol);

exports.getProfitLossFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsSplitRepo.getByMasterId("profit_loss", masterStockId);

exports.getProfitLossFundamentalsBySymbol = async (symbol) =>
  fundamentalsSplitRepo.getBySymbol("profit_loss", symbol);

exports.getBalanceSheetFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsSplitRepo.getByMasterId("balance_sheet", masterStockId);

exports.getBalanceSheetFundamentalsBySymbol = async (symbol) =>
  fundamentalsSplitRepo.getBySymbol("balance_sheet", symbol);

exports.getCashFlowFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsSplitRepo.getByMasterId("cash_flow", masterStockId);

exports.getCashFlowFundamentalsBySymbol = async (symbol) =>
  fundamentalsSplitRepo.getBySymbol("cash_flow", symbol);

exports.getRatiosFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsSplitRepo.getByMasterId("ratios", masterStockId);

exports.getRatiosFundamentalsBySymbol = async (symbol) =>
  fundamentalsSplitRepo.getBySymbol("ratios", symbol);

exports.getShareholdingFundamentalsByMasterId = async (masterStockId) =>
  fundamentalsSplitRepo.getByMasterId("shareholdings", masterStockId);

exports.getShareholdingFundamentalsBySymbol = async (symbol) =>
  fundamentalsSplitRepo.getBySymbol("shareholdings", symbol);
