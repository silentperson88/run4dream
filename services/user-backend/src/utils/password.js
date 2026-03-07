// utils/password.js
const bcrypt = require("bcryptjs");

exports.hashPassword = async (password) => {
  return bcrypt.hash(password, 12);
};

exports.comparePassword = async (password, hash) => {
  return bcrypt.compare(password, hash);
};
