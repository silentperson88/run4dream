const jwt = require("jsonwebtoken");
const { pool } = require("../config/db");
const { response } = require("../utils/response.utils");
const { MESSAGES } = require("../utils/constants/response.constants");

exports.authMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return response(res, 401, MESSAGES.AUTH.UNAUTHORIZED);
    }

    const token = authHeader.split(" ")[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    const userId = Number(decoded.userId || decoded.id);
    if (!Number.isFinite(userId) || userId <= 0) {
      return response(res, 401, MESSAGES.AUTH.INVALID_TOKEN);
    }

    const { rows } = await pool.query(
      `
        SELECT
          id,
          email,
          role,
          is_active,
          wallet_fund,
          total_fund_added,
          total_fund_withdrawn
        FROM users
        WHERE id = $1
        LIMIT 1
      `,
      [userId],
    );

    const user = rows[0];
    if (!user || !user.is_active) {
      return response(res, 401, MESSAGES.AUTH.USER_INACTIVE);
    }

    req.user = {
      id: user.id,
      email: user.email,
      role: user.role,
      isActive: !!user.is_active,
      wallet_fund: Number(user.wallet_fund || 0),
      total_fund_added: Number(user.total_fund_added || 0),
      total_fund_withdrawn: Number(user.total_fund_withdrawn || 0),
    };

    return next();
  } catch (err) {
    return response(
      res,
      401,
      err.name === "TokenExpiredError"
        ? MESSAGES.AUTH.TOKEN_EXPIRED
        : MESSAGES.AUTH.INVALID_TOKEN,
    );
  }
};
