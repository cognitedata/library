---
name: dm-limits-and-best-practices
description: "CDF Data Modeling limits: concurrency (429s), pagination, batching, search vs filter, QueuedTaskRunner, and the cap on LLM calls over query results (5 default, 50 max, cached). Triggers: DMS limits, 429, pagination, nextCursor, cdfTaskRunner, instances.query, chat completions over query results."
allowed-tools: Read, Glob, Grep, Edit, Write
metadata:
  argument-hint: ""
---

# CDF Data Modeling: Limits, Concurrency & Best Practices

This is a reference skill. When writing or reviewing code that calls CDF Data Modeling APIs, apply the patterns below.

This skill owns runtime reliability concerns: limits, concurrency, retries, throughput, and batching behavior.
For traversal payload correctness and graph-specific failure signatures, see `dm-graph-traversal`.

---

## DMS Limits Reference

For the latest concurrency limits, resource limits, and property value limits, see the official documentation:
**https://docs.cognite.com/cdf/dm/dm_reference/dm_limits_and_restrictions**

Key things to be aware of:
- Instance **apply**, **delete**, and **query** operations each have their own concurrent request limits
- Exceeding these limits returns **429 Too Many Requests**
- Transformations consume a large portion of the concurrency budget, leaving less for other clients
- `instances.list` has a max page size (use pagination for complete results)
- `instances.query` table expressions each have their own item limit
- `instances.upsert` accepts up to 1000 items per call
- `in` filters accept at most 1000 values per expression; larger sets must be split into batches

---

## Search vs Filter: When to Use Which

### `instances.search` — Free-text search on text properties

Use `instances.search` when you need fuzzy/text matching on string fields (names, descriptions, etc.). It supports an `operator` parameter:

- **`AND`** (default) — Narrow search. All terms must match. Use when the user provides a specific query.
- **`OR`** — Broad "shotgun" search. Any term can match. Use for exploratory/typeahead search where you want maximum recall.

```typescript
// Narrow search: find a specific cell by name (AND — all terms must match)
const exactResults = await client.instances.search({
  view: { type: 'view', ...PROCESS_CELL_VIEW },
  query: 'reactor tank A',
  properties: ['name'],
  operator: 'AND',
  limit: 10,
});

// Broad search: typeahead/autocomplete (OR — any term can match)
const broadResults = await client.instances.search({
  view: { type: 'view', ...BATCH_VIEW },
  query: 'BUDE completed',
  properties: ['name', 'description', 'batchStatus'],
  operator: 'OR',
  limit: 10,
});
```

You can combine `search` with `filter` to further constrain results with exact-match conditions:

```typescript
// Text search + exact filter: search for "pump" but only in active nodes
const filtered = await client.instances.search({
  view: { type: 'view', ...PROCESS_CELL_VIEW },
  query: 'pump',
  properties: ['name', 'description'],
  filter: {
    equals: {
      property: getContainerProperty(MY_CONTAINER, 'status'),
      value: 'active',
    },
  },
  limit: 20,
});
```

### `instances.list` / `instances.query` with `filter` — Exact-match filtering

Use `filter` when you need precise, deterministic matching (equals, range, in, hasData, etc.). No fuzzy matching — values must match exactly.

```typescript
// Exact match: get all completed batches
const completedBatches = await client.instances.list({
  instanceType: 'node',
  sources: [{ source: { type: 'view', ...BATCH_VIEW } }],
  filter: {
    equals: {
      property: getContainerProperty(BATCH_CONTAINER, 'batchStatus'),
      value: 'completed',
    },
  },
  limit: 1000,
});
```

### Decision Guide

| Need                                | Use                           |
| ----------------------------------- | ----------------------------- |
| User typing in a search box         | `instances.search` with `OR`  |
| Find a specific item by name        | `instances.search` with `AND` |
| Filter by status, date range, enums | `filter` on list/query        |
| Text search + exact constraints     | `instances.search` + `filter` |

### `in` filter value limit (1000) and batching

CDF `in` filters support a maximum of 1000 values in a single filter expression. If you need to filter against more than 1000 IDs, split values into chunks and issue multiple requests, then merge results.

```typescript
const IN_FILTER_BATCH_SIZE = 1000;
// Reuse the Chunking Utility defined in the Batching Write Operations section.

async function listByExternalIds(
  client: CogniteClient,
  externalIds: string[],
): Promise<NodeOrEdge[]> {
  const idBatches = chunk(externalIds, IN_FILTER_BATCH_SIZE);
  const responses = await Promise.all(
    idBatches.map((batch) =>
      cdfTaskRunner.schedule(() =>
        client.instances.list({
          instanceType: 'node',
          sources: [{ source: { type: 'view', ...MY_VIEW } }],
          filter: {
            in: {
              property: ['node', 'externalId'],
              values: batch,
            },
          },
          limit: 1000,
        })
      )
    )
  );

  return responses.flatMap((r) => r.items);
}
```

---

## QueuedTaskRunner (Semaphore)

**Always use the global `cdfTaskRunner`** to wrap CDF API calls. It limits concurrent requests and prevents 429 errors and deadlocks.

### Source Code

If the project does not already have a semaphore utility, create `src/shared/utils/semaphore.ts` with this implementation:

```typescript
/**
 * AbortError thrown when a queued task is cancelled
 */
export class AbortError extends Error {
  public constructor(message: string = 'Aborted') {
    super(message);
    this.name = 'AbortError';
  }
}

type PendingTask<AsyncFn, AsyncFnResult> = {
  resolve: (result: AsyncFnResult) => void;
  reject: (error: unknown) => void;
  fn: AsyncFn;
  key?: string;
};

const DEFAULT_MAX_CONCURRENT_TASKS = 15;

/**
 * QueuedTaskRunner for controlling concurrent operations
 * Used to limit concurrent CDF API requests to avoid rate limiting and deadlocks
 * Essentially a semaphore that allows a limited number of tasks to run at once.
 */
export default class QueuedTaskRunner<
  AsyncFn extends () => Promise<AsyncFnResult>,
  AsyncFnResult = Awaited<ReturnType<AsyncFn>>,
> {
  private pendingTasks: PendingTask<AsyncFn, AsyncFnResult>[] = [];
  private currentPendingTasks: number = 0;
  private readonly maxConcurrentTasks: number = 1;

  public constructor(
    maxConcurrentTasks: number = DEFAULT_MAX_CONCURRENT_TASKS
  ) {
    this.maxConcurrentTasks = maxConcurrentTasks;
  }

  public schedule(
    fn: AsyncFn,
    options: { key?: string } = {}
  ): Promise<AsyncFnResult> {
    this.startTrackingTime();

    return new Promise((resolve, reject) => {
      if (options.key !== undefined) {
        // Cancel existing tasks with the same key (deduplication)
        this.pendingTasks
          .filter((task) => task.key === options.key)
          .forEach((task) => task.reject(new AbortError()));

        this.pendingTasks = this.pendingTasks.filter(
          (task) => task.key !== options.key
        );
      }

      this.pendingTasks.push({
        resolve,
        reject,
        fn,
        key: options.key,
      });

      this.attemptConsumingNextTask();
    });
  }

  public async attemptConsumingNextTask(): Promise<void> {
    if (this.pendingTasks.length === 0) return;
    if (this.currentPendingTasks >= this.maxConcurrentTasks) return;

    const pendingTask = this.pendingTasks.shift();
    if (pendingTask === undefined) {
      throw new Error('pendingTask is undefined, this should never happen');
    }

    this.currentPendingTasks++;
    const { fn, resolve, reject } = pendingTask;

    try {
      const result = await fn();
      resolve(result);
    } catch (e) {
      reject(e);
    } finally {
      this.currentPendingTasks--;
      this.tick();
      this.attemptConsumingNextTask();
    }
  }

  public clearQueue = (): void => {
    this.pendingTasks = [];
  };

  private startTime: number | null = null;

  private startTrackingTime = (): void => {
    if (this.startTime === null) {
      this.startTime = performance.now();
    }
  };

  private tick = (): void => {
    if (this.pendingTasks.length === 0) {
      this.startTime = null;
    }
  };
}

/**
 * Global task runner for CDF API requests
 * Limits concurrent requests to avoid 429 rate limiting and deadlocks
 */
export const cdfTaskRunner = new QueuedTaskRunner(DEFAULT_MAX_CONCURRENT_TASKS);
```

### Usage Pattern

Always wrap CDF calls with `cdfTaskRunner.schedule()`:

```typescript
import { cdfTaskRunner } from '../../../../shared/utils/semaphore';

// Single query
export async function fetchBatches(client: CogniteClient): Promise<CDFBatch[]> {
  return cdfTaskRunner.schedule(async () => {
    const response = await client.instances.query({
      with: { /* ... */ },
      select: { /* ... */ },
    });
    return response.items?.nodes || [];
  });
}

// Multiple parallel queries (safe — the semaphore limits concurrency)
export async function enrichBatch(
  client: CogniteClient,
  batch: CDFBatch
): Promise<BatchEnrichment> {
  const [currentOp, lastOp, cells, material] = await Promise.all([
    fetchCurrentOperation(client, batch.space, batch.externalId),
    fetchLastCompletedOperation(client, batch.space, batch.externalId),
    fetchProcessCells(client, batch.space, batch.externalId),
    fetchMaterial(client, batch.space, batch.externalId),
  ]);
  return { currentOp, lastOp, cells, material };
}

// Each of the above functions internally uses cdfTaskRunner.schedule(),
// so Promise.all is safe — the semaphore prevents exceeding concurrency limits
```

### Deduplication with Keys

Use the `key` option to cancel stale requests when the same query is triggered again (e.g., user changes filters quickly):

```typescript
const result = await cdfTaskRunner.schedule(
  async () => client.instances.query({ /* ... */ }),
  { key: `batch-flow-${batchId}` }
);
// If another call with the same key arrives before this completes,
// the previous pending call is rejected with AbortError
```

---

## Pagination

DMS `instances.list` returns at most `limit` items and a `nextCursor` for the next page.
DMS `instances.query` uses a `cursors` object keyed by table expression name.

### instances.list Pagination

```typescript
async function fetchAllNodes(client: CogniteClient): Promise<CDFNodeResponse[]> {
  const allItems: CDFNodeResponse[] = [];
  let cursor: string | undefined = undefined;

  do {
    const response = await client.instances.list({
      instanceType: 'node',
      sources: [{ source: { type: 'view', ...MY_VIEW } }],
      filter: {
        equals: {
          property: getContainerProperty(MY_CONTAINER, 'status'),
          value: 'active',
        },
      },
      limit: 1000,
      cursor,
    });

    allItems.push(...response.items);
    cursor = response.nextCursor;
  } while (cursor);

  return allItems;
}
```

### instances.query Pagination

The `query` endpoint returns `nextCursor` as a `Record<string, string>` (one cursor per table expression). Use it via the `cursors` parameter:

```typescript
import { isEmpty } from 'lodash';

async function fetchAllResults(
  client: CogniteClient
): Promise<{ results: CDFResult[]; edges: EdgeDefinition[] }> {
  const QUERY_LIMIT = 10_000;

  const fetchPage = async (
    nextCursors?: Record<string, string>
  ): Promise<{ results: CDFResult[]; edges: EdgeDefinition[] }> => {
    const { items, nextCursor } = await client.instances.query({
      with: {
        results: {
          limit: QUERY_LIMIT,
          nodes: {
            filter: {
              hasData: [{ type: 'view', ...RESULT_VIEW }],
            },
          },
        },
        relatedEdges: {
          limit: QUERY_LIMIT,
          edges: {
            from: 'results' as const,
            maxDistance: 1,
            direction: 'outwards' as const,
            filter: {
              equals: {
                property: ['edge', 'type'],
                value: MY_EDGE_TYPE,
              },
            },
          },
        },
      },
      cursors: nextCursors, // Pass cursors from previous page
      select: {
        results: {
          sources: [
            { source: { type: 'view', ...RESULT_VIEW }, properties: ['*'] },
          ],
        },
        relatedEdges: {},
      },
    });

    const results = (items?.results || []) as CDFResult[];
    const edges = (items?.relatedEdges || []).filter(
      (e) => e.instanceType === 'edge'
    );

    // Recurse if more pages exist
    if (!isEmpty(nextCursor)) {
      const next = await fetchPage(nextCursor);
      return {
        results: [...results, ...next.results],
        edges: [...edges, ...next.edges],
      };
    }

    return { results, edges };
  };

  return fetchPage();
}
```

### Pagination + QueuedTaskRunner Combined

Always wrap paginated fetches with the semaphore to avoid saturating the concurrency budget:

```typescript
export async function fetchAllWithPagination(
  client: CogniteClient
): Promise<CDFNodeResponse[]> {
  return cdfTaskRunner.schedule(async () => {
    const allItems: CDFNodeResponse[] = [];
    let cursor: string | undefined = undefined;

    do {
      const response = await client.instances.list({
        instanceType: 'node',
        sources: [{ source: { type: 'view', ...MY_VIEW } }],
        filter: { /* ... */ },
        limit: 1000,
        cursor,
      });

      allItems.push(...response.items);
      cursor = response.nextCursor;

      // Optional: break early if you have enough data
      if (allItems.length >= 500) break;
    } while (cursor);

    return allItems;
  });
}
```

---

## Batching Write Operations

When upserting many instances, chunk them to stay under the apply concurrency limit. Each `instances.upsert` call accepts up to 1000 items.

### Chunking Utility

```typescript
function chunk<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}
```

### Batched Upsert with QueuedTaskRunner

```typescript
const UPSERT_BATCH_SIZE = 1000;

async function batchUpsertNodes(
  client: CogniteClient,
  nodes: NodeOrEdgeCreate[]
): Promise<void> {
  const chunks = chunk(nodes, UPSERT_BATCH_SIZE);

  // Process chunks through the semaphore — safe even with Promise.all
  await Promise.all(
    chunks.map((batch) =>
      cdfTaskRunner.schedule(async () => {
        await client.instances.upsert({
          items: batch,
        });
      })
    )
  );
}
```

### Batched Delete with QueuedTaskRunner

Instance deletes have an even stricter concurrency limit. Use a separate, more restrictive task runner:

```typescript
import QueuedTaskRunner from '../../../../shared/utils/semaphore';

// Dedicated runner for deletes (stricter concurrency — check docs for current limit)
const deleteTaskRunner = new QueuedTaskRunner(2);

async function batchDeleteNodes(
  client: CogniteClient,
  nodeIds: { space: string; externalId: string }[]
): Promise<void> {
  const chunks = chunk(nodeIds, 1000);

  for (const batch of chunks) {
    await deleteTaskRunner.schedule(async () => {
      await client.instances.delete(
        batch.map((id) => ({
          instanceType: 'node' as const,
          ...id,
        }))
      );
    });
  }
}
```

---

## Hard gate — LLM calls over query results

Do not map chat completions over `instances.list` / `query` / `search` hits. Prefer one Atlas / EOS sidebar turn (`integrate-fusion-agent`). If per-item completions are required: **5** per user action, ceiling **50**, cache by `space:externalId:lastUpdatedTime`, user-initiated only.

---

## Common Pitfalls

### 1. Deadlocks from Nested Semaphore Calls

If function A holds a semaphore slot and calls function B which also needs a slot, you can deadlock if all slots are occupied. **Keep the semaphore at the outermost call level**, or ensure inner calls don't go through the same semaphore.

```typescript
// BAD: Nested semaphore — can deadlock
async function fetchAndEnrich(client: CogniteClient) {
  return cdfTaskRunner.schedule(async () => {
    const batches = await fetchBatches(client); // This also calls cdfTaskRunner.schedule!
    // If all slots are held by fetchAndEnrich callers, fetchBatches will never run
  });
}

// GOOD: Let inner functions own the semaphore
async function fetchAndEnrich(client: CogniteClient) {
  const batches = await fetchBatches(client); // Has its own semaphore call
  const enriched = await Promise.all(
    batches.map((b) => enrichBatch(client, b)) // Each has its own semaphore call
  );
  return enriched;
}
```

### 2. Forgetting Pagination

DMS returns at most `limit` items. If you don't paginate, you silently lose data. Always check `nextCursor`:

```typescript
// BAD: May miss data
const response = await client.instances.list({ limit: 1000, /* ... */ });
const items = response.items; // Could be incomplete!

// GOOD: Paginate
const allItems = [];
let cursor;
do {
  const response = await client.instances.list({ limit: 1000, cursor, /* ... */ });
  allItems.push(...response.items);
  cursor = response.nextCursor;
} while (cursor);
```

### 3. Unbounded Promise.all Without Semaphore

Firing many parallel API calls will hit the 429 limit immediately:

```typescript
// BAD: Too many simultaneous requests
await Promise.all(batchIds.map((id) => client.instances.query({ /* ... */ })));

// GOOD: Each call goes through the semaphore
await Promise.all(
  batchIds.map((id) =>
    cdfTaskRunner.schedule(() => client.instances.query({ /* ... */ }))
  )
);
```

### 4. Query Limit per Table Expression

Each table expression in `instances.query` has its own `limit`. If your traversal might return more items than the limit in a single expression, you must paginate using the `cursors` parameter.

### 5. Oversized `in` Filters

`in` filters are capped at 1000 values per expression. Passing more than 1000 values in a single `in` filter can fail or produce incomplete behavior depending on endpoint/version. Always chunk the values and run batched requests.

---

## Summary Checklist

- [ ] Wrap all CDF API calls with `cdfTaskRunner.schedule()`
- [ ] Paginate `instances.list` calls using `cursor` / `nextCursor`
- [ ] Paginate `instances.query` calls using `cursors` / `nextCursor` when data may exceed limits
- [ ] Chunk write operations to 1000 items per `instances.upsert` call
- [ ] Use a separate, stricter task runner for deletes
- [ ] Avoid nesting `cdfTaskRunner.schedule()` calls to prevent deadlocks
- [ ] Use `Promise.all` with semaphore-wrapped functions, never with raw API calls
- [ ] Use `instances.search` for text matching, `filter` for exact-match queries
- [ ] Split `in` filter values into batches of at most 1000 and merge responses
- [ ] LLM-over-query-results capped (5 / max 50) and cached, or not present
- [ ] Refer to https://docs.cognite.com/cdf/dm/dm_reference/dm_limits_and_restrictions for current limits
