import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, loadEnv } from "vite";
import mkcert from "vite-plugin-mkcert";

import { fusionOpenPlugin } from "@cognite/app-sdk/vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const runtimeMode = env.VITE_RUNTIME_MODE;
  const isHostMode = runtimeMode === "cdf_host";
  const plugins = [react(), mkcert(), tailwindcss()];

  if (isHostMode) {
    plugins.push(fusionOpenPlugin());
  }

  return {
    base: "./",
    plugins,
    resolve: {
      alias: {
        "@/components/ui": path.resolve(__dirname, "./src/shared/components/ui"),
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 3001,
      server: {
        sourcemap: true,
      },
      proxy: {
        "/api": {
          target: env.VITE_PROXY_TARGET || "http://localhost:7071",
          changeOrigin: true,
        },
      },
    },
    build: {
      sourcemap: true,
    },
    worker: {
      format: "es",
    },
  };
});

