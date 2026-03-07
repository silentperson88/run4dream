const jwt = require("jsonwebtoken");
const { responseUtils } = require("../utils/Constants/responseContants.utils");

module.exports = (req, res, next) => {
  const expected = process.env.SUPERADMIN_KEY;
  const provided = req.headers["x-superadmin-key"];
  const authHeader = req.headers.authorization || "";
  const jwtSecret = process.env.JWT_SECRET;
  const requiredRole = (process.env.JWT_SUPERADMIN_ROLE || "SUPERADMIN").toUpperCase();

  // Keep legacy superadmin-key auth path.
  if (expected && provided && provided === expected) {
    return next();
  }

  // Fallback to JWT superadmin auth path.
  if (!jwtSecret) {
    return res.status(500).json({
      success: false,
      message: "JWT_SECRET is not configured",
    });
  }

  if (!authHeader.startsWith("Bearer ")) {
    return res.status(401).json({
      success: false,
      message: responseUtils.UNAUTHORIZED_ACCESS,
    });
  }

  try {
    const token = authHeader.slice(7);
    const decoded = jwt.verify(token, jwtSecret);
    const role = String(decoded?.role || decoded?.user?.role || "").toUpperCase();

    if (role !== requiredRole) {
      return res.status(403).json({
        success: false,
        message: responseUtils.UNAUTHORIZED_ACCESS,
      });
    }

    return next();
  } catch (err) {
    return res.status(401).json({
      success: false,
      message: responseUtils.UNAUTHORIZED_ACCESS,
    });
  }
};
