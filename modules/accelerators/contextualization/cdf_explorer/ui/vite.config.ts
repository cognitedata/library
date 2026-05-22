import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const apiProxy = process.env.VITE_API_PROXY ?? "http://127.0.0.1:8785";

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
