import { describe, expect, it } from "vitest";

import {
	adaptiveDownsample,
	downsample,
	lttbDownsample,
	minMaxDownsample,
} from "./downsampling";

const series = Array.from({ length: 20 }, (_, index) => ({
	x: index,
	y: index === 10 ? 100 : index % 5,
}));

describe("lttbDownsample", () => {
	it("returns the original series when the target is large enough", () => {
		expect(lttbDownsample(series, 20)).toBe(series);
	});

	it("keeps at most two points when the target is two or fewer", () => {
		expect(lttbDownsample(series, 2)).toHaveLength(2);
		expect(lttbDownsample(series, 0)).toHaveLength(2);
		expect(lttbDownsample(series, 1)).toHaveLength(2);
	});

	it("always keeps the first and last points", () => {
		const sampled = lttbDownsample(series, 6);
		expect(sampled[0]).toEqual(series[0]);
		expect(sampled.at(-1)).toEqual(series.at(-1));
		expect(sampled.length).toBeLessThanOrEqual(6);
	});
});

describe("minMaxDownsample", () => {
	it("returns the original series when it already fits", () => {
		expect(minMaxDownsample(series.slice(0, 3), 8)).toHaveLength(3);
	});

	it("keeps a peak and a valley in x order", () => {
		const data = [
			{ x: 0, y: 1 },
			{ x: 1, y: 9 },
			{ x: 2, y: 0 },
			{ x: 3, y: 4 },
		];
		const sampled = minMaxDownsample(data, 2);
		expect(sampled.map((point) => point.y)).toEqual([9, 0]);
	});

	it("emits a single point when min and max are the same sample", () => {
		const flat = [
			{ x: 0, y: 1 },
			{ x: 1, y: 1 },
		];
		expect(minMaxDownsample(flat, 1)).toEqual([flat[0]]);
	});
});

describe("adaptiveDownsample", () => {
	it("returns the original series when it already fits", () => {
		expect(adaptiveDownsample(series.slice(0, 4), 8)).toHaveLength(4);
	});

	it("keeps an extreme peak", () => {
		const sampled = adaptiveDownsample(series, 8, 0.95);
		expect(sampled.some((point) => point.y === 100)).toBe(true);
	});

	it("falls back to LTTB when half the points are extreme", () => {
		const noisy = series.map((point, index) => ({
			...point,
			y: index % 2 === 0 ? 0 : 100,
		}));
		const sampled = adaptiveDownsample(noisy, 6, 0.5);
		expect(sampled[0]).toEqual(noisy[0]);
		expect(sampled.at(-1)).toEqual(noisy.at(-1));
	});
});

describe("downsample", () => {
	it("dispatches each mode", () => {
		expect(downsample(series, 5, "lttb").length).toBeGreaterThan(0);
		expect(downsample(series, 5, "minmax").length).toBeGreaterThan(0);
		expect(downsample(series, 5, "adaptive").length).toBeGreaterThan(0);
		expect(downsample(series, 5).length).toBeGreaterThan(0);
	});
});
