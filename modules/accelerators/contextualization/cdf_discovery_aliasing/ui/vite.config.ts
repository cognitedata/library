import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/** Dev proxy target; override if you change cdf_discovery API port from default 8785. */
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
