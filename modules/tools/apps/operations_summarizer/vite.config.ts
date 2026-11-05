import fs from "node:fs";
import path from "node:path";
import { fusionOpenPlugin, manifestCspPlugin } from "@cognite/app-sdk/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";
import mkcert from "vite-plugin-mkcert";

const PDF_WORKER_URL = "/node_modules/pdfjs-dist/build/pdf.worker.min.mjs";

/** Vite's transform injects `@vite/client` into this module. Loaded as a worker, that client reloads the page. */
function servePdfWorkerRaw(): Plugin {
  const workerFile = path.resolve(
    import.meta.dirname,
    "node_modules/pdfjs-dist/build/pdf.worker.min.mjs",
  );
  return {
    name: "serve-pdf-worker-raw",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const [pathname, query = ""] = (req.url ?? "").split("?");
        // `?url` is the import that must stay a Vite module (`export default "<path>"`).
        // Only the worker script request itself is served raw.
        if (pathname !== PDF_WORKER_URL || query.includes("url")) {
          next();
          return;
        }
        res.setHeader("Content-Type", "text/javascript");
        fs.createReadStream(workerFile).pipe(res);
      });
    },
  };
}

export default defineConfig({
  base: "./",
  plugins: [
    servePdfWorkerRaw(),
    manifestCspPlugin(),
    react(),
    mkcert(),
    fusionOpenPlugin(),
    tailwindcss(),
  ],
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
    // Tailwind @source scans Aura's dist. Watching those files makes style.css
    // invalidate in a loop (Vite reconnects) when a summary view loads PDF/CSS.
    watch: {
      ignored: ["**/node_modules/**", "**/.git/**"],
    },
    // Vite's default CORS origin is localhost only. Fusion probes
    // https://localhost:3001/__probe from the host origin.
    cors: {
      origin: [
        /^https:\/\/[\w-]+\.fusion\.cognite\.com$/,
        /^https?:\/\/(?:(?:[^:]+\.)?localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/,
      ],
    },
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
    cors: {
      origin: [
        /^https:\/\/[\w-]+\.fusion\.cognite\.com$/,
        /^https?:\/\/(?:(?:[^:]+\.)?localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/,
      ],
    },
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
