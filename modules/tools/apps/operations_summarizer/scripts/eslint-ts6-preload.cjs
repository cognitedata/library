const Module = require("node:module");
const path = require("node:path");

const typescript6Entry = require.resolve("typescript6");
const originalResolve = Module._resolveFilename;

// typescript-eslint refuses TypeScript 7. Keep the app on TypeScript 7 and
// give only those packages the TypeScript 6 build they still import.
Module._resolveFilename = function resolveFilename(request, parent, isMain, options) {
	const parentFile = parent?.filename ?? "";
	const isTypescriptEslint =
		parentFile.includes(`${path.sep}@typescript-eslint${path.sep}`) ||
		parentFile.includes(`${path.sep}typescript-eslint${path.sep}`) ||
		parentFile.includes(`${path.sep}ts-api-utils${path.sep}`);
	if (request === "typescript" && isTypescriptEslint) {
		return typescript6Entry;
	}
	return originalResolve.call(this, request, parent, isMain, options);
};
