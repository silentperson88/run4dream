const dotenv = require("dotenv");
dotenv.config();

const env = {
  port: process.env.PORT,
  pgDatabaseUrl: process.env.PG_DATABASE_URL,
  jwtSecret: process.env.JWT_SECRET,
  jwtExpiresIn: process.env.JWT_EXPIRES_IN,
};

module.exports = { env };
