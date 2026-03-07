const { LoginService } = require("../services/smartapi.service");
const { getLastEntry } = require("../services/token.service");
const { response } = require("../utils/response.utils");
const constantUtils = require("../utils/constants.utils");
const { isSameDay } = require("../utils/Mthods.utils");
const MARKET_CONFIG = require("../config/market.config");

const loginService = new LoginService();

/**
 * POST /api/login/totp
 * Body: { totp: "123456" }
 */
exports.loginWithTOTP = async (req, res) => {
  try {
    const { data } = req.body;

    // get totp, open time, and close time (TOTP-9:15-15:30)
    const [totp, open_time, close_time] = data.split("-");

    if (!totp) {
      return res.status(400).json({
        success: false,
        message: "TOTP is required",
      });
    }

    const tokenBody = {
      totp,
      market: {
        open_time: open_time || MARKET_CONFIG.MARKET_OPEN_TIME,
        close_time: close_time || MARKET_CONFIG.MARKET_CLOSE_TIME,
      },

      scheduler: {
        state: "NOT_STARTED",
        current_phase: "NONE",
      },
    };

    // Generate token via service
    const tokenData = await loginService.generateTokenWithTOTP(tokenBody);

    return res.json({
      success: true,
      message: "Login successful. Token generated and stored.",
      data: {
        generated_at: tokenData.generated_at || new Date(),
        expiry_time: tokenData.expiry_time,
      },
    });
  } catch (error) {
    return res.status(401).json({
      success: false,
      message: error.message || "Login failed",
    });
  }
};

// check server is logged in today or not
exports.checkLoginStatus = async (req, res) => {
  try {
    const loginData = await getLastEntry();

    if (!loginData) {
      return response(res, 200, constantUtils.SERVER_NOT_LOGIN, {
        isOnline: false,
      });
    }

    const now = Date.now();

    const generatedAt = loginData.generated_at;
    const expiryAt = loginData.expiry_time;

    const isToday = isSameDay(generatedAt, now);

    const isExpired = !isToday || now > expiryAt;

    if (isExpired) {
      return response(res, 200, constantUtils.SERVER_NOT_LOGIN, {
        isOnline: false,
      });
    }

    return response(res, 200, constantUtils.SERVER_ONLINE, {
      isOnline: true,
    });
  } catch (error) {
    return response(res, 500, constantUtils.ERROR, error);
  }
};
