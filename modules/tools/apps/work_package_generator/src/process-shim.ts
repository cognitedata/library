/**
 * Browser shim for Node's `process` global.
 * Exposes Vite's mode as `process.env.NODE_ENV` for libraries that read it.
 * Must be imported first in main.tsx.
 */
const g = typeof globalThis !== "undefined" ? globalThis : window;
const env: Record<string, string> = {};
try {
	// Expose Vite's mode as NODE_ENV for libraries that check it
	if (typeof import.meta !== "undefined" && import.meta.env) {
		env.NODE_ENV = import.meta.env.MODE ?? "development";
	}
} catch {
	env.NODE_ENV = "development";
}
type GlobalWithProcess = typeof globalThis & {
	process?: { env?: Record<string, string>; [key: string]: unknown };
};
const globalWithProcess = g as GlobalWithProcess;
globalWithProcess.process = {
	...globalWithProcess.process,
	env: { ...globalWithProcess.process?.env, ...env },
};
