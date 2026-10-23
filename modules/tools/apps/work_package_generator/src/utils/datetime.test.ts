import { describe, expect, it, vi } from "vitest";
import {
	formatDate,
	nowIsoMs,
	safeEndOfDayISOString,
	safeParseDate,
	safeToISOString,
} from "./datetime";

describe("nowIsoMs", () => {
	it("returns ISO string", () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2024-06-01T12:00:00.000Z"));
		expect(nowIsoMs()).toBe("2024-06-01T12:00:00.000Z");
		vi.useRealTimers();
	});
});

describe("formatDate", () => {
	it("returns null for falsy", () => {
		expect(formatDate()).toBeNull();
		expect(formatDate("")).toBeNull();
	});

	it("formats valid ISO date", () => {
		const s = formatDate("2024-01-15T10:00:00.000Z");
		expect(s).toBeTruthy();
		expect(s).toContain("2024");
	});

	it("returns raw string on parse failure", () => {
		vi.spyOn(Date.prototype, "toLocaleDateString").mockImplementation(() => {
			throw new Error("bad");
		});
		expect(formatDate("not-a-date")).toBe("not-a-date");
	});

	it("returns raw string for invalid date strings", () => {
		// These create Invalid Date objects instead of throwing errors
		expect(formatDate("invalid-date")).toBe("invalid-date");
		expect(formatDate("2024-13-45")).toBe("2024-13-45"); // Invalid month/day
		expect(formatDate("not a date at all")).toBe("not a date at all");
		expect(formatDate("2024-01-01T25:00:00Z")).toBe("2024-01-01T25:00:00Z"); // Invalid hour
	});

	it("handles edge cases properly", () => {
		// Empty string should return null (handled by falsy check)
		expect(formatDate("")).toBeNull();

		// Valid dates should format correctly
		expect(formatDate("2024-01-01T00:00:00Z")).toContain("2024");
		expect(formatDate("2024-12-31T23:59:59Z")).toContain("2024");
	});
});

describe("safeParseDate", () => {
	it("returns null for invalid inputs", () => {
		expect(safeParseDate(null)).toBeNull();
		expect(safeParseDate(undefined)).toBeNull();
		expect(safeParseDate("")).toBeNull();
		expect(safeParseDate("   ")).toBeNull();
		expect(safeParseDate("invalid-date")).toBeNull();
		expect(safeParseDate("2024-13-45")).toBeNull();
		expect(safeParseDate("not a date at all")).toBeNull();
	});

	it("returns valid Date objects for valid inputs", () => {
		const validInputs = [
			"2024-01-15",
			"2024-01-15T10:00:00Z",
			"2024-01-15T10:00:00.000Z",
			"January 15, 2024",
		];

		validInputs.forEach((input) => {
			const result = safeParseDate(input);
			expect(result).toBeInstanceOf(Date);
			expect(result?.getTime()).not.toBeNaN();
		});
	});

	it("trims whitespace from input", () => {
		const result = safeParseDate("  2024-01-15  ");
		expect(result).toBeInstanceOf(Date);
		expect(result?.getFullYear()).toBe(2024);
	});
});

describe("safeToISOString", () => {
	it("returns null for invalid inputs", () => {
		expect(safeToISOString(null)).toBeNull();
		expect(safeToISOString(undefined)).toBeNull();
		expect(safeToISOString("")).toBeNull();
		expect(safeToISOString("invalid-date")).toBeNull();
	});

	it("returns ISO string for valid inputs", () => {
		const result = safeToISOString("2024-01-15T10:00:00Z");
		expect(result).toBe("2024-01-15T10:00:00.000Z");
	});

	it("handles various date formats", () => {
		const result = safeToISOString("January 15, 2024");
		// "January 15, 2024" is parsed as local midnight, which can land on
		// 2024-01-14 or 2024-01-15 in UTC depending on the runner's timezone.
		expect(result).toMatch(/2024-01-(14|15)T\d{2}:\d{2}:\d{2}\.\d{3}Z/);
	});
});

describe("safeEndOfDayISOString", () => {
	it("returns null for invalid inputs", () => {
		expect(safeEndOfDayISOString(null)).toBeNull();
		expect(safeEndOfDayISOString(undefined)).toBeNull();
		expect(safeEndOfDayISOString("")).toBeNull();
		expect(safeEndOfDayISOString("invalid-date")).toBeNull();
	});

	it("sets time to end of day and returns ISO string", () => {
		const result = safeEndOfDayISOString("2024-01-15");
		expect(result).toMatch(/2024-01-15T23:59:59\.999Z/);
	});

	it("preserves date but changes time to end of day", () => {
		// Even if input has a specific time, it should be changed to end of day
		const result = safeEndOfDayISOString("2024-01-15T10:30:00Z");
		expect(result).toMatch(/2024-01-15T23:59:59\.999Z/);
	});

	it("handles edge case of leap year", () => {
		const result = safeEndOfDayISOString("2024-02-29"); // 2024 is a leap year
		expect(result).toMatch(/2024-02-29T23:59:59\.999Z/);
	});
});
