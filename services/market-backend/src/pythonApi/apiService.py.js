import axios from "axios";

const PYTHON_BASE_URL =
  process.env.PYTHON_API_BASE_URL || "http://localhost:8005";

const API_PREFIX = "api/v1";

export const pythonApi = axios.create({
  baseURL: `${PYTHON_BASE_URL}${API_PREFIX}`,
  timeout: 30000,
  headers: {
    "Content-Type": "application/json",
  },
});

/**
 * Global error handling
 */
pythonApi.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error("[Python API Error]", {
      url: error.config?.url,
      status: error.response?.status,
      data: error.response?.data,
    });

    throw error;
  }
);
