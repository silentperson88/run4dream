const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const toNullableNumber = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);
const ensureObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

const syncSerialSequence = async (db, tableName, idColumn = "id") => {
  const sequenceRes = await db.query(
    `SELECT pg_get_serial_sequence($1, $2) AS sequence_name`,
    [tableName, idColumn],
  );
  const sequenceName = sequenceRes.rows[0]?.sequence_name;
  if (!sequenceName) return null;

  await db.query(
    `
      SELECT setval(
        $1,
        COALESCE((SELECT MAX(${idColumn}) FROM ${tableName}), 0) + 1,
        false
      )
    `,
    [sequenceName],
  );

  return sequenceName;
};

module.exports = {
  toNumber,
  toNullableNumber,
  ensureArray,
  ensureObject,
  syncSerialSequence,
};
