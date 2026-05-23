import path from "node:path";
import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const uiDir = path.dirname(fileURLToPath(import.meta.url));
const apiProxy = process.env.VITE_API_PROXY ?? "http://127.0.0.1:8770";

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
