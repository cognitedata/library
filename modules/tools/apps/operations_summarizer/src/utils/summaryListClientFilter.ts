import type { SummaryInstance } from "@/types/operations";

/** Cursor sentinel when the next UI page is satisfied from overflow only (no server cursor). */
export const SUMMARY_LIST_OVERFLOW_CURSOR = "__overflow__";

export interface SummaryServerPageResult {
  items: SummaryInstance[];
  nextCursor: string | undefined;
  usedListApi: boolean;
  rawCount: number;
}

export interface CollectFilteredSummaryPageParams {
  pageSize: number;
  serverCursor: string | undefined;
  overflow: SummaryInstance[];
  fetchPage: (cursor: string | undefined) => Promise<SummaryServerPageResult>;
  matches: (summary: SummaryInstance) => boolean;
  maxExtraFetches?: number;
}

export interface CollectFilteredSummaryPageResult {
  items: SummaryInstance[];
  overflow: SummaryInstance[];
  /** Server cursor to resume after this UI page, or overflow sentinel when only overflow remains. */
  nextCursor: string | undefined;
}

function resolveServerCursor(cursor: string | undefined): string | undefined {
  return cursor === SUMMARY_LIST_OVERFLOW_CURSOR ? undefined : cursor;
}

/**
 * Accumulates client-side-filtered summaries until a UI page is full or the server
 * result set is exhausted. Preserves overflow for the next page so filtered pagination
 * does not under-fill or skip rows at page boundaries.
 */
export async function collectFilteredSummaryPage(
  params: CollectFilteredSummaryPageParams,
): Promise<CollectFilteredSummaryPageResult> {
  const maxExtraFetches = params.maxExtraFetches ?? 10;
  const pool = [...params.overflow];
  const collected: SummaryInstance[] = [];
  let serverCursor = resolveServerCursor(params.serverCursor);
  let fetches = 0;

  while (collected.length < params.pageSize && fetches <= maxExtraFetches) {
    while (pool.length > 0 && collected.length < params.pageSize) {
      const next = pool.shift();
      if (next) collected.push(next);
    }

    if (collected.length >= params.pageSize) {
      break;
    }

    const page = await params.fetchPage(serverCursor);
    fetches += 1;

    for (const item of page.items) {
      if (!params.matches(item)) continue;
      if (collected.length < params.pageSize) {
        collected.push(item);
      } else {
        pool.push(item);
      }
    }

    const isLastServerPage =
      (page.usedListApi && page.rawCount < params.pageSize) || !page.nextCursor;
    serverCursor = isLastServerPage ? undefined : page.nextCursor;

    if (isLastServerPage) {
      break;
    }
  }

  const nextCursor =
    pool.length > 0 || serverCursor
      ? (serverCursor ?? SUMMARY_LIST_OVERFLOW_CURSOR)
      : undefined;

  return {
    items: collected,
    overflow: pool,
    nextCursor,
  };
}
