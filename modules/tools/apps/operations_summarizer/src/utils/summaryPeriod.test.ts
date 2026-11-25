import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import {
	getSummaryPeriod,
	parseLocalCalendarDate,
	periodOverlaps,
} from "./summaryPeriod";

function summary(externalId: string): SummaryInstance {
	return {
		externalId,
		space: "ops_summaries",
		createdTime: new Date(2026, 10, 25, 10).getTime(),
		properties: {},
	};
}

function localDay(y: number, m: number, d: number): Date {
	return new Date(y, m - 1, d);
}

describe("parseLocalCalendarDate", () => {
	it("parses YYYY-MM-DD as local midnight", () => {
		expect(parseLocalCalendarDate("2026-11-01")).toEqual(localDay(2026, 11, 1));
	});

	it("returns null for invalid input", () => {
		expect(parseLocalCalendarDate("2026-13-40")).toBeNull();
		expect(parseLocalCalendarDate("nope")).toBeNull();
	});
});

describe("getSummaryPeriod", () => {
	it("reads the period saved in the external id", () => {
		const period = getSummaryPeriod(
			summary("ops_summary_2026-11-01_2026-11-24_2026-11-25_10_00_00_1")
		);
		expect(period.start).toEqual(localDay(2026, 11, 1));
		expect(period.end).toEqual(localDay(2026, 11, 24));
	});

	it("falls back to the created day when the id has no period", () => {
		const period = getSummaryPeriod(summary("legacy"));
		expect(period.start).toEqual(localDay(2026, 11, 25));
		expect(period.end).toEqual(localDay(2026, 11, 25));
	});
});

describe("periodOverlaps", () => {
	const period = { start: localDay(2026, 11, 1), end: localDay(2026, 11, 24) };

	it("includes a filter that starts on the period end day", () => {
		expect(
			periodOverlaps(period, {
				start: localDay(2026, 11, 24),
				end: localDay(2026, 11, 30),
			})
		).toBe(true);
	});

	it("includes a filter whose end day is the period start day", () => {
		expect(
			periodOverlaps(period, {
				start: localDay(2026, 10, 1),
				end: localDay(2026, 11, 1),
			})
		).toBe(true);
	});

	it("excludes a filter fully after the period", () => {
		expect(
			periodOverlaps(period, {
				start: localDay(2026, 11, 25),
				end: localDay(2026, 11, 30),
			})
		).toBe(false);
	});

	it("excludes a filter fully before the period", () => {
		expect(
			periodOverlaps(period, {
				start: localDay(2026, 10, 1),
				end: localDay(2026, 10, 31),
			})
		).toBe(false);
	});
});
