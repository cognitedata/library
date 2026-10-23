import { afterEach, describe, expect, it } from "vitest";

type GlobalWithProcess = typeof globalThis & {
	process?: { env?: Record<string, string> };
};

describe("process-shim", () => {
	const previous = (globalThis as GlobalWithProcess).process;

	afterEach(() => {
		(globalThis as GlobalWithProcess).process = previous;
	});

	it("exposes Vite's mode as process.env.NODE_ENV", async () => {
		(globalThis as GlobalWithProcess).process = { env: { EXISTING: "kept" } };
		await import("./process-shim");
		const env = (globalThis as GlobalWithProcess).process?.env;
		expect(env?.NODE_ENV).toBe(import.meta.env.MODE ?? "development");
		expect(env?.EXISTING).toBe("kept");
	});
});
