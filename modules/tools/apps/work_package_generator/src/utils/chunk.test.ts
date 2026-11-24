import { describe, expect, it } from "vitest";
import { chunk } from "./chunk";

describe("chunk", () => {
	it("returns empty for empty array", () => {
		expect(chunk([], 3)).toEqual([]);
	});

	it("splits into slices of at most size", () => {
		expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
	});

	it("throws error for non-positive size", () => {
		expect(() => chunk([1, 2, 3], 0)).toThrow("Chunk size must be positive");
		expect(() => chunk([1, 2, 3], -1)).toThrow("Chunk size must be positive");
	});
});
