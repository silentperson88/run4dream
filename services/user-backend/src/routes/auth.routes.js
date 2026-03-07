// routes/authRoutes.js
const express = require("express");
const router = express.Router();

const authController = require("../controllers/auth.controller");
const validateRequest = require("../middlewares/validateRequest.middleware");
const {
  registerValidator,
  loginValidator,
  verifyEmailValidator,
  forgotPasswordValidator,
  resetPasswordValidator,
  resendVerificationOtpValidator,
} = require("../validator/authValidators");

router.post(
  "/register",
  registerValidator,
  validateRequest,
  authController.register,
);

router.post("/login", loginValidator, validateRequest, authController.login);

router.post(
  "/verify-email",
  verifyEmailValidator,
  validateRequest,
  authController.verifyEmail,
);

router.post(
  "/forgot-password",
  forgotPasswordValidator,
  validateRequest,
  authController.forgotPassword,
);

router.post(
  "/reset-password",
  resetPasswordValidator,
  validateRequest,
  authController.resetPassword,
);

router.post(
  "/resend-verification-otp",
  resendVerificationOtpValidator,
  validateRequest,
  authController.resendVerificationOtp,
);

module.exports = router;
