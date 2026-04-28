import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/** Dev proxy target; cdf_access_control defaults to API 8775 / Vite 5183 — override if you change ports. */
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
