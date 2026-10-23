import { describe, expect, it } from "vitest";
import {
	getErrorMessage,
	looksLikeTechnicalErrorMessage,
	toUserFacingErrorMessage,
	USER_FACING_ERROR_FALLBACK,
} from "./errors";

describe("getErrorMessage", () => {
	it("handles Error, string, and fallback", () => {
		expect(getErrorMessage(new Error("e"))).toBe("e");
		expect(getErrorMessage("plain")).toBe("plain");
		expect(getErrorMessage(404)).toBe("Unknown error");
		expect(getErrorMessage(null, "fb")).toBe("fb");
		expect(getErrorMessage({ message: "from object" })).toBe("from object");
		expect(getErrorMessage({ message: 4 })).toBe("Unknown error");
	});
});

describe("looksLikeTechnicalErrorMessage", () => {
	it("returns true for URLs", () => {
		expect(
			looksLikeTechnicalErrorMessage(
				"Failed to load http://localhost:5173/src/style.css"
			)
		).toBe(true);
	});

	it("returns true for chunk and css asset references", () => {
		expect(
			looksLikeTechnicalErrorMessage(
				"error loading dynamically imported module: /assets/chunk-abc123.css?import"
			)
		).toBe(true);
	});

	it("returns true for Windows paths", () => {
		expect(
			looksLikeTechnicalErrorMessage(
				"ENOENT: C:\\Users\\dev\\project\\file.css"
			)
		).toBe(true);
	});

	it("returns false for typical API messages", () => {
		expect(looksLikeTechnicalErrorMessage("Work order not found")).toBe(false);
		expect(
			looksLikeTechnicalErrorMessage("Request failed with status code 403")
		).toBe(false);
	});
});

describe("toUserFacingErrorMessage", () => {
	it("replaces technical messages with fallback", () => {
		expect(
			toUserFacingErrorMessage(
				"Unable to preload CSS for /assets/index-xyz.css"
			)
		).toBe(USER_FACING_ERROR_FALLBACK);
	});

	it("preserves plain-language messages", () => {
		expect(toUserFacingErrorMessage("No work order found.")).toBe(
			"No work order found."
		);
	});

	it("returns fallback for whitespace-only input", () => {
		expect(toUserFacingErrorMessage("   ")).toBe(USER_FACING_ERROR_FALLBACK);
	});
});
