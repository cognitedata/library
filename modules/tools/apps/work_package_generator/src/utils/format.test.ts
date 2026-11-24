import { describe, expect, it } from "vitest";
import { formatPropertyValue, getColumnTitle } from "./format";

describe("getColumnTitle", () => {
	it("maps known keys", () => {
		expect(getColumnTitle("sourceUnit")).toBe("Unit");
		expect(getColumnTitle("mimeType")).toBe("MIME Type");
		expect(getColumnTitle("externalId")).toBe("External ID");
	});

	it("returns prop for unknown keys", () => {
		expect(getColumnTitle("foo")).toBe("foo");
	});
});

describe("formatPropertyValue", () => {
	it("handles nullish and primitives", () => {
		expect(formatPropertyValue(null)).toBe("-");
		expect(formatPropertyValue(undefined)).toBe("-");
		expect(formatPropertyValue("")).toBe("-");
		expect(formatPropertyValue("x")).toBe("x");
		expect(formatPropertyValue(42)).toBe("42");
		expect(formatPropertyValue(true)).toBe("true");
	});

	it("formats arrays", () => {
		expect(formatPropertyValue([])).toBe("-");
		expect(formatPropertyValue([1, 2])).toBe("1, 2");
	});

	it("formats objects with common keys", () => {
		expect(formatPropertyValue({ name: "N" })).toBe("N");
		expect(formatPropertyValue({ label: "L" })).toBe("L");
		expect(formatPropertyValue({ externalId: "e" })).toBe("e");
		expect(formatPropertyValue({ value: 0 })).toBe("0");
	});

	it("falls back to entries", () => {
		expect(formatPropertyValue({ a: 1, b: null })).toBe("1");
		expect(formatPropertyValue({ a: 1, b: 2 })).toContain("a:");
		expect(formatPropertyValue({})).toBe("-");
	});
});
