const jwt = require("jsonwebtoken");
const { response } = require("../utils/response.utils");

exports.authMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return response(res, 401, "Unauthorized access");
    }

    const token = authHeader.split(" ")[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    req.user = {
      id: Number(decoded?.userId || decoded?.id || 0),
      email: decoded?.email || "",
      role: decoded?.role || "USER",
    };

    return next();
  } catch (err) {
    return response(res, 401, err?.name === "TokenExpiredError" ? "Authentication token expired" : "Invalid authentication token");
  }
};
