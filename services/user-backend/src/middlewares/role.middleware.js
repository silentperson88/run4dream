// middleware/roleMiddleware.js
exports.requireSuperAdmin = (req, res, next) => {
  if (req.user.role !== "SUPERADMIN") {
    return res.status(403).json({ message: "Access denied" });
  }
  next();
};
