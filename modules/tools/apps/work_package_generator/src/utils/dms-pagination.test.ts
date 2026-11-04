import { describe, expect, it, vi } from "vitest";
import {
	fetchPagesWithItemCap,
	readOptionalNextCursor,
} from "./dms-pagination";

describe("readOptionalNextCursor", () => {
	it("returns a non-empty string cursor", () => {
		expect(readOptionalNextCursor({ nextCursor: "page-2" })).toBe("page-2");
	});

	it("ignores missing, empty, or non-string cursors", () => {
		expect(readOptionalNextCursor(undefined)).toBeUndefined();
		expect(readOptionalNextCursor({})).toBeUndefined();
		expect(readOptionalNextCursor({ nextCursor: "" })).toBeUndefined();
		expect(readOptionalNextCursor({ nextCursor: 12 })).toBeUndefined();
	});
});

describe("fetchPagesWithItemCap", () => {
	it("returns all items when under cap", async () => {
		const fetchPage = vi
			.fn()
			.mockResolvedValueOnce({
				items: [1, 2],
				nextCursor: "c1",
			})
			.mockResolvedValueOnce({ items: [3], nextCursor: undefined });

		const { items, truncated } = await fetchPagesWithItemCap({
			maxTotalItems: 100,
			warnContext: "test",
			fetchPage,
		});

		expect(items).toEqual([1, 2, 3]);
		expect(truncated).toBe(false);
		expect(fetchPage).toHaveBeenCalledTimes(2);
	});

	it("truncates when a page exceeds remaining room", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

		const fetchPage = vi.fn().mockResolvedValue({
			items: [1, 2, 3, 4, 5],
			nextCursor: "more",
		});

		const { items, truncated } = await fetchPagesWithItemCap({
			maxTotalItems: 3,
			warnContext: "test",
			fetchPage,
		});

		expect(items).toEqual([1, 2, 3]);
		expect(truncated).toBe(true);
		expect(warn).toHaveBeenCalled();
		warn.mockRestore();
	});

	it("stops when full after a page and nextCursor exists", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

		const fetchPage = vi
			.fn()
			.mockResolvedValueOnce({
				items: [1, 2],
				nextCursor: "c1",
			})
			.mockResolvedValueOnce({
				items: [3, 4],
				nextCursor: "c2",
			});

		const { items, truncated } = await fetchPagesWithItemCap({
			maxTotalItems: 4,
			warnContext: "test",
			fetchPage,
		});

		expect(items).toEqual([1, 2, 3, 4]);
		expect(truncated).toBe(true);
		expect(warn).toHaveBeenCalled();
		warn.mockRestore();
	});
});
