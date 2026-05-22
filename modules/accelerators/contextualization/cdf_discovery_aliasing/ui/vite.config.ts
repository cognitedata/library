import path from "node:path";
import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const uiDir = path.dirname(fileURLToPath(import.meta.url));

/** Dev proxy target; cdf_access_control defaults to API 8775 / Vite 5183 — override if you change ports. */
const apiProxy = process.env.VITE_API_PROXY ?? "http://127.0.0.1:8765";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@accelerators/brand": path.resolve(uiDir, "../../../shared/brand"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: apiProxy,
        changeOrigin: true,
      },
    },
  },
});
