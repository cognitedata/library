import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import mkcert from "vite-plugin-mkcert";

import { fusionOpenPlugin } from "@cognite/dune/vite";

export default defineConfig({
  base: "./",
  plugins: [react(), mkcert(), fusionOpenPlugin(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 3001,
  },
  worker: {
    format: "es",
  },
});

