import { afterEach, describe, expect, it } from "vitest";

describe("process-shim", () => {
	const globalProcess = globalThis as {
		process?: { env?: Record<string, string> };
	};
	const previous = globalProcess.process;

	afterEach(() => {
		globalProcess.process = previous;
	});

	it("exposes Vite's mode as process.env.NODE_ENV", async () => {
		globalProcess.process = { env: { EXISTING: "kept" } };
		await import("./process-shim");
		const env = globalProcess.process?.env;
		expect(env?.NODE_ENV).toBe(import.meta.env.MODE ?? "development");
		expect(env?.EXISTING).toBe("kept");
	});
});
