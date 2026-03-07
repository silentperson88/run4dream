const { SmartAPI } = require("smartapi-javascript");
const tokensRepo = require("../repositories/tokens.repository");
const tokenService = require("../services/token.service");
const queueService = require("./queueService.service");

const API_KEY = process.env.SMARTAPI_API_KEY;
const CLIENT_CODE = process.env.SMARTAPI_CLIENT_CODE;
const PASSWORD = process.env.SMARTAPI_PASSWORD;

class LoginService {
  constructor() {
    this.smartApi = new SmartAPI({ api_key: API_KEY });
    this.tokenCache = null;
  }

  async generateTokenWithTOTP(tokenBody) {
    const totpValue = tokenBody.totp;
    const loginData = await this.smartApi.generateSession(
      CLIENT_CODE,
      PASSWORD,
      totpValue,
    );

    if (loginData.status === false) {
      throw new Error(loginData.message);
    }

    const tokenObj = {
      access_token: loginData.data.jwtToken,
      refresh_token: loginData.data.refreshToken,
      feedToken: loginData.data.feedToken,
      ...tokenBody,
      expiry_time: new Date(Date.now() + 24 * 60 * 60 * 1000),
      totp: totpValue,
      is_active: true,
      generated_by: "api",
    };

    const tokenDoc = await tokensRepo.create(tokenObj);
    this.tokenCache = tokenDoc;

    return tokenDoc;
  }

  async refreshToken() {
    if (!this.tokenCache || !this.tokenCache.refresh_token) {
      throw new Error("No refresh token available. Login required.");
    }

    const refreshData = await this.smartApi.refreshToken(
      this.tokenCache.refresh_token,
    );

    const tokenObj = {
      access_token: refreshData.access_token,
      refresh_token: refreshData.refresh_token,
      feedToken: this.tokenCache.feedToken,
      expiry_time: new Date(Date.now() + refreshData.expires_in * 1000),
      totp: this.tokenCache.totp,
      market: this.tokenCache.market,
      scheduler: this.tokenCache.scheduler,
      is_active: true,
      generated_by: "api",
    };

    const token = await tokensRepo.create(tokenObj);
    this.tokenCache = token;
    return token;
  }

  async getAccessToken() {
    if (
      this.tokenCache &&
      this.tokenCache.access_token &&
      new Date(this.tokenCache.expiry_time).getTime() > Date.now()
    ) {
      return {
        access_token: this.tokenCache.access_token,
        id: this.tokenCache.id,
      };
    }

    const latestToken = await tokensRepo.getLastEntry();
    if (latestToken && new Date(latestToken.expiry_time).getTime() > Date.now()) {
      this.tokenCache = latestToken;
      return {
        access_token: this.tokenCache.access_token,
        id: this.tokenCache.id,
      };
    }

    if (latestToken) {
      this.tokenCache = latestToken;
      try {
        const refreshed = await this.refreshToken();
        return {
          access_token: refreshed.access_token,
          id: refreshed.id,
        };
      } catch (_) {
        throw new Error("Token expired. Please provide new TOTP.");
      }
    }

    throw new Error(
      "No valid token found. Please provide TOTP to generate token.",
    );
  }
}

const loginService = new LoginService();

class SmartApiPriceService {
  constructor() {
    this.smartApi = new SmartAPI({ api_key: API_KEY });
    this.allowedModes = ["LTP", "OHLC", "FULL"];
  }

  async getMarketData(mode, tokenIds, exchange = "NSE") {
    if (!this.allowedModes.includes(mode)) {
      throw new Error(`Invalid mode. Allowed: ${this.allowedModes.join(", ")}`);
    }

    if (!Array.isArray(tokenIds) || tokenIds.length === 0) {
      throw new Error("tokenIds must be a non-empty array");
    }

    if (tokenIds.length > 50) {
      throw new Error("Maximum 50 tokenIds allowed");
    }

    return queueService.enqueue(async () => {
      const accessToken = await loginService.getAccessToken();
      this.smartApi.access_token = accessToken.access_token;
      console.log(`Fetching ${mode} data for tokens:`, tokenIds);

      const response = await this.smartApi.marketData({
        mode,
        exchangeTokens: {
          [exchange]: tokenIds,
        },
      });

      if (!response.success && response.errorCode === "AG8001") {
        await tokenService.inactivateToken(accessToken.id);
        throw new Error("Token expired. Please regenerate it.");
      }

      return response;
    });
  }

  getLTP(tokenIds) {
    return this.getMarketData("LTP", tokenIds, "NSE");
  }

  getOHLC(tokenIds) {
    return this.getMarketData("OHLC", tokenIds, "NSE");
  }

  getFULL(tokenIds) {
    return this.getMarketData("FULL", tokenIds, "NSE");
  }

  async getHistoricalCandleData({
    exchange = "NSE",
    symboltoken,
    interval = "ONE_DAY",
    fromdate,
    todate,
  }) {
    if (!symboltoken) throw new Error("symboltoken is required");

    return queueService.enqueue(async () => {
      const accessToken = await loginService.getAccessToken();
      this.smartApi.access_token = accessToken.access_token;

      const response = await this.smartApi.getCandleData({
        exchange,
        symboltoken,
        interval,
        fromdate,
        todate,
      });

      if (!response.success && response.errorCode === "AG8001") {
        await tokenService.inactivateToken(accessToken.id);
        throw new Error("Token expired. Please regenerate it.");
      }

      return response;
    });
  }
}

module.exports = { SmartApiPriceService, LoginService };
