import path from "node:path";
import { fusionOpenPlugin, manifestCspPlugin } from "@cognite/app-sdk/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import mkcert from "vite-plugin-mkcert";

export default defineConfig({
  base: "./",
  plugins: [manifestCspPlugin(), react(), mkcert(), fusionOpenPlugin(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "./src"),
    },
  },
  // ─── DEV SERVER ONLY ──────────────────────────────────────────────────────
  // These headers are served by `vite dev` and have NO effect on production
  // builds. Production HTTP headers are controlled by the Flows/Fusion platform.
  // 'unsafe-eval' and 'unsafe-inline' are required here for React's dev runtime
  // and Vite's HMR; they must never appear in a production CSP.
  server: {
    port: 3001,
    headers: {
      "X-Content-Type-Options": "nosniff",
      "Content-Security-Policy": [
        "default-src 'self'",
        // Production build emits plain ES modules — eval and inline scripts are not needed
        "script-src 'self' blob: 'unsafe-inline' 'unsafe-eval'",
        // Tailwind injects inline styles; Aura loads fonts from Google Fonts
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "worker-src blob: 'self'",
        "connect-src 'self' blob: https://*.cognitedata.com wss:",
        // Podcast <audio> plays signed CDF file download URLs
        "media-src 'self' blob: https://*.cognitedata.com",
        "img-src 'self' data: blob:",
        "font-src 'self' data: https://fonts.gstatic.com",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ].join("; "),
    },
  },

  // ─── PREVIEW SERVER (production build served locally via `vite preview`) ──
  // Mirrors a production-safe CSP: no unsafe-eval, no unsafe-inline for scripts.
  // This catches CSP regressions before the build is deployed.
  preview: {
    port: 3001,
    headers: {
      "X-Content-Type-Options": "nosniff",
      "Content-Security-Policy": [
        "default-src 'self'",
        // Production build emits plain ES modules — eval and inline scripts are not needed
        "script-src 'self' blob:",
        // Tailwind injects inline styles; Aura loads fonts from Google Fonts
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "worker-src blob: 'self'",
        "connect-src 'self' blob: https://*.cognitedata.com wss:",
        // Podcast <audio> plays signed CDF file download URLs
        "media-src 'self' blob: https://*.cognitedata.com",
        "img-src 'self' data: blob:",
        "font-src 'self' data: https://fonts.gstatic.com",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ].join("; "),
    },
  },
  worker: {
    format: "es",
  },
  optimizeDeps: {
    exclude: ["onnxruntime-web"],
  },
});
