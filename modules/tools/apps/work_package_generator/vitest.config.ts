import { mergeConfig } from "vite";
import { defineConfig } from "vitest/config";

import viteConfig from "./vite.config.ts";

export default mergeConfig(
	viteConfig,
	defineConfig({
		test: {
			globals: true,
			environment: "happy-dom",
			setupFiles: ["./vitest.setup.ts"],
			// Aura reads `import.meta.env.VITEST`. Vitest leaves node_modules
			// untransformed, so that object is missing unless Aura is inlined.
			server: {
				deps: {
					inline: ["@cognite/aura"],
				},
			},
			exclude: ["**/node_modules/**", "**/.agents/**", "**/.claude/**"],
			// Limit concurrent workers to reduce peak memory usage.
			// Each worker can consume several hundred MB; without a cap
			// the full test suite can exceed 4 GB and OOM on developer machines.
			// (Vitest 4 removed `poolOptions`; these are now top-level options.)
			maxWorkers: 4,
			coverage: {
				provider: "v8",
				reporter: ["text", "text-summary", "lcov"],
				include: ["src/**/*.{ts,tsx}"],
				exclude: [
					"**/*.d.ts",
					"**/*.{test,spec}.{ts,tsx}",
					"src/main.tsx",
					"src/**/vite-env.d.ts",
					/** Test doubles — allowed coverage exclude */
					"src/__mocks__/**",
				],
				thresholds: {
					lines: 80,
					branches: 80,
				},
			},
		},
	})
);
