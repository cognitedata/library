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
