import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/** Dev proxy target; override when another app already uses 8765 (e.g. cdf_access_control). */
const apiProxy = process.env.VITE_API_PROXY ?? "http://127.0.0.1:8765";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: apiProxy,
        changeOrigin: true,
      },
    },
  },
});
