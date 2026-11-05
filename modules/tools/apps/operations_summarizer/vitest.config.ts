import path from "node:path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "./src"),
    },
  },
  test: {
    globals: true,
    environment: "happy-dom",
    setupFiles: ["./vitest.setup.ts"],
    server: {
      deps: {
        // Aura reads import.meta.env at module scope. Inline so Vite injects env.
        inline: ["@cognite/aura"],
      },
    },
    include: ["src/**/*.{test,spec}.{ts,tsx}", "tests/**/*.{test,spec}.{ts,tsx}"],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.agents/**",
      "**/.claude/**",
    ],
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary", "html"],
      include: ["src/**/*.{ts,tsx}"],
      exclude: [
        "**/*.{test,spec}.{ts,tsx}",
        "**/vite-env.d.ts",
        "src/main.tsx",
      ],
      thresholds: {
        lines: 80,
      },
    },
  },
});
