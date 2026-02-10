import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, loadEnv } from "vite";

import { fusionOpenPlugin } from "@cognite/dune/vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const isStandalone = env.VITE_STANDALONE === "true";

  return {
    base: "./",
    plugins: [
      react(),
      ...(isStandalone ? [] : [fusionOpenPlugin()]),
      tailwindcss(),
    ],
    envPrefix: ["VITE_"],
    define: {
      "import.meta.env.CDF_PROJECT": JSON.stringify(env.CDF_PROJECT),
      "import.meta.env.CDF_URL": JSON.stringify(env.CDF_URL),
      "import.meta.env.IDP_TENANT_ID": JSON.stringify(env.IDP_TENANT_ID),
      "import.meta.env.IDP_CLIENT_ID": JSON.stringify(env.IDP_CLIENT_ID),
      "import.meta.env.IDP_SCOPES": JSON.stringify(env.IDP_SCOPES),
      "import.meta.env.CDF_PROXY_URL": JSON.stringify(env.CDF_PROXY_URL),
    },
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 4242,
    },
    worker: {
      format: "es",
    },
  };
});

