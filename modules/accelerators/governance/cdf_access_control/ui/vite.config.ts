import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiProxy = process.env.VITE_API_PROXY || "http://127.0.0.1:8775";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5183,
    proxy: {
      "/api": { target: apiProxy, changeOrigin: true },
    },
  },
});
