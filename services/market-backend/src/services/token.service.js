const tokensRepo = require("../repositories/tokens.repository");

const getLastEntry = async () => tokensRepo.getLastEntry();

const updateTokenEntry = async (data) => {
  if (!data?.id) throw new Error("id is required");
  return tokensRepo.updateById(data.id, data);
};

const inactivateToken = async (id) => tokensRepo.inactivateById(id);

module.exports = { getLastEntry, updateTokenEntry, inactivateToken };
