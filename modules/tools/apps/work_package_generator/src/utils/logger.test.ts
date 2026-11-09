import { afterEach, describe, expect, it, vi } from "vitest";
import { logError, logWarn } from "./logger";

describe("logError", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("serializes Error, object-with-message, primitive, and omits empty context", () => {
		const spy = vi.spyOn(console, "error").mockImplementation(() => {});

		const errNoStack = new Error("no-stack");
		delete (errNoStack as Error & { stack?: string }).stack;
		logError({ scope: "t", message: "a", cause: errNoStack });
		logError({ scope: "t", message: "b", cause: { message: "nested" } });
		logError({
			scope: "t",
			message: "b2",
			cause: { message: 123 } as unknown,
		});
		logError({ scope: "t", message: "c", cause: 404 });
		logError({ scope: "t", message: "d", context: {} });
		logError({ scope: "t", message: "e" });

		expect(spy.mock.calls.length).toBeGreaterThanOrEqual(6);
		const withError = JSON.parse(spy.mock.calls[0][0] as string);
		expect(withError.error?.message).toBe("no-stack");
		expect(withError.error?.stack).toBeUndefined();
		const noCause = JSON.parse(
			spy.mock.calls[spy.mock.calls.length - 1][0] as string
		);
		expect(noCause.error).toBeUndefined();
	});
});

describe("logWarn", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("writes a warn-level JSON line", () => {
		const spy = vi.spyOn(console, "warn").mockImplementation(() => {});

		logWarn({ scope: "dms", message: "truncated", context: { cap: 3 } });

		const entry = JSON.parse(spy.mock.calls[0][0] as string);
		expect(entry.level).toBe("warn");
		expect(entry.scope).toBe("dms");
		expect(entry.message).toBe("truncated");
		expect(entry.context).toEqual({ cap: 3 });
	});
});
