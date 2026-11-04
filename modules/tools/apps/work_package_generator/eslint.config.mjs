import { auraEslintPlugin } from "@cognite/aura/eslint";
import tseslint from "typescript-eslint";

// The app compiles with TypeScript 7. scripts/eslint-ts6-preload.cjs points
// typescript-eslint at the TypeScript 6 API, which is all this config needs.
export default tseslint.config(
	{
		ignores: ["dist", "build", "coverage"],
	},
	{
		files: ["**/*.{ts,tsx}"],
		languageOptions: {
			parser: tseslint.parser,
		},
		plugins: {
			aura: auraEslintPlugin,
		},
		rules: {
			"aura/no-overriding-styles": "warn",
		},
	},
);
