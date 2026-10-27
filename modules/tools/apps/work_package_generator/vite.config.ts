import fs from "node:fs";
import path from "node:path";
import {
	fusionOpenPlugin,
	manifestCspPlugin,
	mkcertPlugin,
} from "@cognite/app-sdk/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";

const duneCanvasBase = path.resolve(import.meta.dirname, "src/canvas-preview");

/** Copies root app.json next to index.html so the running app can fetch it. */
function serveAppJsonPlugin(): Plugin {
	const file = path.resolve(import.meta.dirname, "app.json");
	return {
		name: "serve-app-json",
		configureServer(server) {
			server.middlewares.use((req, res, next) => {
				const url = req.url?.split("?")[0];
				if (url !== "/app.json") {
					next();
					return;
				}
				res.setHeader("Content-Type", "application/json");
				res.setHeader("Cache-Control", "no-store");
				res.end(fs.readFileSync(file, "utf8"));
			});
		},
		generateBundle() {
			this.emitFile({
				type: "asset",
				fileName: "app.json",
				source: fs.readFileSync(file, "utf8"),
			});
		},
	};
}

export default defineConfig({
	base: "./",
	plugins: [
		// The SDK reporter is an inline <script>. Fusion production CSP is
		// script-src 'self', which blocks it. Dev server only.
		{ ...manifestCspPlugin(), apply: "serve" },
		react(),
		mkcertPlugin(),
		fusionOpenPlugin(),
		serveAppJsonPlugin(),
		tailwindcss(),
	],
	resolve: {
		alias: [
			{ find: "@", replacement: path.resolve(import.meta.dirname, "./src") },
			{
				find: "@app.json",
				replacement: path.resolve(import.meta.dirname, "./app.json"),
			},
			{
				find: "@dune-canvas/canvas-preview-lib",
				replacement: `${duneCanvasBase}/canvas-preview-lib/src/index.ts`,
			},
			{
				find: "@dune-canvas/canvas-lib",
				replacement: `${duneCanvasBase}/canvas-lib/src/index.ts`,
			},
			{
				find: "@dune-canvas/canvas-app",
				replacement: `${duneCanvasBase}/canvas-app/src/index.ts`,
			},
			{
				find: /^@dune-canvas\/canvas-plugins\/(.+)/,
				replacement: `${duneCanvasBase}/canvas-plugins/src/$1`,
			},
			{
				find: "@dune-canvas/canvas-plugins",
				replacement: `${duneCanvasBase}/canvas-plugins/src/index.ts`,
			},
		],
	},
	optimizeDeps: {
		exclude: ["@dune-canvas/canvas-plugins"],
		include: [
			"pdfjs-dist",
			"uplot",
			"@preact/signals-react",
			"@preact/signals-react/runtime",
			"use-sync-external-store",
			"use-sync-external-store/shim",
			"use-sync-external-store/shim/with-selector",
		],
	},
	server: {
		port: 3001,
		headers: {
			"Permissions-Policy": "clipboard-write=*",
		},
	},
	worker: {
		format: "es",
	},
});
