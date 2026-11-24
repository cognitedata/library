import { describe, expect, it } from "vitest";

import { computeStats, formatTimestamp } from "./helpers";

describe("formatTimestamp", () => {
	it("includes the date and a GMT offset", () => {
		expect(formatTimestamp(Date.UTC(2026, 0, 2, 3, 4, 5))).toMatch(
			/02\.01\.2026|01\.01\.2026/
		);
		expect(formatTimestamp(0)).toContain("GMT(");
	});
});

describe("computeStats", () => {
	it("returns null for an empty series", () => {
		expect(computeStats([])).toBeNull();
	});

	it("averages raw points", () => {
		expect(
			computeStats([
				{ x: 1, y: 1 },
				{ x: 2, y: 3 },
			])
		).toEqual({ min: "1.000", max: "3.000", average: "2.000", count: 2 });
	});

	it("uses server aggregates and a zero count fallback", () => {
		expect(
			computeStats([
				{ x: 1, y: 2, min: 1, max: 4, count: 2 },
				{ x: 2, y: 3, min: 0, max: 5, count: 2 },
			])
		).toMatchObject({ min: "0.000", max: "5.000", count: 4 });
		expect(
			computeStats([{ x: 1, y: 2, min: 1, max: 4, count: 0 }])?.average
		).toBe("0.000");
	});
});
