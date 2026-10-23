import { afterEach, describe, expect, it, vi } from "vitest";
import { copyToClipboard } from "./clipboard";

describe("copyToClipboard", () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it("uses execCommand even when navigator.clipboard is available", async () => {
		// copyToClipboard always uses the execCommand fallback (works inside
		// Fusion/Flows iframes where the Permissions-Policy may block the async
		// Clipboard API).
		const writeText = vi.fn().mockResolvedValue(undefined);
		vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });

		const textarea = document.createElement("textarea");
		const selectSpy = vi.spyOn(textarea, "select").mockImplementation(() => {});
		const createSpy = vi
			.spyOn(document, "createElement")
			.mockReturnValue(textarea);
		const appendSpy = vi
			.spyOn(document.body, "appendChild")
			.mockImplementation((n) => n);
		const removeSpy = vi
			.spyOn(document.body, "removeChild")
			.mockImplementation((n) => n);
		const execSpy = vi.fn().mockReturnValue(true);
		Object.defineProperty(document, "execCommand", {
			value: execSpy,
			configurable: true,
		});

		await copyToClipboard("hello");

		expect(execSpy).toHaveBeenCalledWith("copy");
		expect(writeText).not.toHaveBeenCalled();

		createSpy.mockRestore();
		appendSpy.mockRestore();
		removeSpy.mockRestore();
		selectSpy.mockRestore();
		Reflect.deleteProperty(document, "execCommand");
	});

	it("throws when execCommand returns false (copy not permitted)", async () => {
		const textarea = document.createElement("textarea");
		vi.spyOn(textarea, "select").mockImplementation(() => {});
		vi.spyOn(document, "createElement").mockReturnValue(textarea);
		vi.spyOn(document.body, "appendChild").mockImplementation((n) => n);
		vi.spyOn(document.body, "removeChild").mockImplementation((n) => n);
		const execSpy = vi.fn().mockReturnValue(false); // execCommand fails
		Object.defineProperty(document, "execCommand", {
			value: execSpy,
			configurable: true,
		});

		await expect(copyToClipboard("fail")).rejects.toThrow(
			"Failed to copy text using fallback method"
		);

		Reflect.deleteProperty(document, "execCommand");
	});

	it("uses execCommand when clipboard API is not available", async () => {
		vi.stubGlobal("navigator", { ...navigator, clipboard: undefined });

		const textarea = document.createElement("textarea");
		const selectSpy = vi.spyOn(textarea, "select").mockImplementation(() => {});
		const createSpy = vi
			.spyOn(document, "createElement")
			.mockReturnValue(textarea);
		const appendSpy = vi
			.spyOn(document.body, "appendChild")
			.mockImplementation((n) => n);
		const removeSpy = vi
			.spyOn(document.body, "removeChild")
			.mockImplementation((n) => n);
		const execSpy = vi.fn().mockReturnValue(true);
		Object.defineProperty(document, "execCommand", {
			value: execSpy,
			configurable: true,
		});

		await copyToClipboard("legacy");

		expect(createSpy).toHaveBeenCalledWith("textarea");
		expect(selectSpy).toHaveBeenCalled();
		expect(execSpy).toHaveBeenCalledWith("copy");
		expect(appendSpy).toHaveBeenCalledWith(textarea);
		expect(removeSpy).toHaveBeenCalledWith(textarea);

		createSpy.mockRestore();
		appendSpy.mockRestore();
		removeSpy.mockRestore();
		selectSpy.mockRestore();
		Reflect.deleteProperty(document, "execCommand");
	});
});
