import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import {
	collectFilteredSummaryPage,
	SUMMARY_LIST_OVERFLOW_CURSOR,
} from "./summaryListClientFilter";

function summary(id: string): SummaryInstance {
	return {
		externalId: id,
		space: "ops_summaries",
		createdTime: 1,
		properties: {},
	};
}

describe("collectFilteredSummaryPage", () => {
	it("fills a page from multiple server batches when client filter is selective", async () => {
		const pages = [
			{
				items: [summary("a1"), summary("skip"), summary("skip")],
				nextCursor: "c2",
				usedListApi: false,
				rawCount: 3,
			},
			{
				items: [summary("a2"), summary("a3"), summary("skip")],
				nextCursor: undefined,
				usedListApi: false,
				rawCount: 3,
			},
		];

		const result = await collectFilteredSummaryPage({
			pageSize: 3,
			serverCursor: undefined,
			overflow: [],
			matches: (s) => s.externalId.startsWith("a"),
			fetchPage: async () => {
				const page = pages.shift();
				if (!page) throw new Error("unexpected fetch");
				return page;
			},
		});

		expect(result.items.map((s) => s.externalId)).toEqual(["a1", "a2", "a3"]);
		expect(result.overflow).toEqual([]);
		expect(result.nextCursor).toBeUndefined();
	});

	it("preserves overflow for the next UI page and exposes overflow cursor", async () => {
		const result = await collectFilteredSummaryPage({
			pageSize: 2,
			serverCursor: undefined,
			overflow: [],
			matches: () => true,
			fetchPage: async () => ({
				items: [summary("one"), summary("two"), summary("three")],
				nextCursor: undefined,
				usedListApi: true,
				rawCount: 3,
			}),
		});

		expect(result.items.map((s) => s.externalId)).toEqual(["one", "two"]);
		expect(result.overflow.map((s) => s.externalId)).toEqual(["three"]);
		expect(result.nextCursor).toBe(SUMMARY_LIST_OVERFLOW_CURSOR);
	});

	it("does not refetch server page 1 when only overflow remains (C08)", async () => {
		let fetchCount = 0;
		const result = await collectFilteredSummaryPage({
			pageSize: 10,
			serverCursor: SUMMARY_LIST_OVERFLOW_CURSOR,
			overflow: [summary("left-1"), summary("left-2"), summary("left-3")],
			matches: () => true,
			fetchPage: async () => {
				fetchCount += 1;
				return {
					items: [summary("page1-a"), summary("page1-b")],
					nextCursor: undefined,
					usedListApi: true,
					rawCount: 2,
				};
			},
		});

		expect(fetchCount).toBe(0);
		expect(result.items.map((s) => s.externalId)).toEqual([
			"left-1",
			"left-2",
			"left-3",
		]);
		expect(result.nextCursor).toBeUndefined();
	});

	it("pages through filtered results without repeats or drops (C08)", async () => {
		const serverPages = new Map([
			[
				undefined,
				{
					items: [summary("a"), summary("b"), summary("c")],
					nextCursor: "c2",
					usedListApi: false,
					rawCount: 3,
				},
			],
			[
				"c2",
				{
					items: [summary("d"), summary("e")],
					nextCursor: undefined,
					usedListApi: false,
					rawCount: 2,
				},
			],
		]);
		const fetchPage = async (cursor: string | undefined) => {
			const page = serverPages.get(cursor);
			if (!page) throw new Error(`unexpected fetch: ${cursor}`);
			return page;
		};

		const seen: string[][] = [];
		let serverCursor: string | undefined;
		let overflow: SummaryInstance[] = [];
		for (let i = 0; i < 3; i += 1) {
			const result = await collectFilteredSummaryPage({
				pageSize: 2,
				serverCursor,
				overflow,
				fetchPage,
				matches: () => true,
			});
			seen.push(result.items.map((s) => s.externalId));
			overflow = result.overflow;
			serverCursor = result.nextCursor;
		}

		expect(seen).toEqual([["a", "b"], ["c", "d"], ["e"]]);
		expect(serverCursor).toBeUndefined();
	});

	it("consumes overflow before fetching the next server page", async () => {
		let fetchCount = 0;
		const result = await collectFilteredSummaryPage({
			pageSize: 2,
			serverCursor: "c1",
			overflow: [summary("cached")],
			matches: () => true,
			fetchPage: async () => {
				fetchCount += 1;
				return {
					items: [summary("fresh")],
					nextCursor: undefined,
					usedListApi: true,
					rawCount: 1,
				};
			},
		});

		expect(fetchCount).toBe(1);
		expect(result.items.map((s) => s.externalId)).toEqual(["cached", "fresh"]);
	});
});
