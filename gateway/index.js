require("dotenv").config();
const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");

const app = express();

app.use(
  "/api/v1/ticker",
  createProxyMiddleware({
    target: process.env.MARKET_BACKEND_URL || "http://localhost:8002",
    changeOrigin: true,
    pathRewrite: { "^/api/v1/ticker": "" },
  }),
);

app.use(
  "/api/v1/user",
  createProxyMiddleware({
    target: process.env.USER_BACKEND_URL || "http://localhost:8001",
    changeOrigin: true,
    pathRewrite: { "^/api/v1/user": "" },
  }),
);

const port = Number(process.env.PORT || 8000);
app.listen(port, () => console.log(`Gateway listening on ${port}`));
