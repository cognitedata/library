---
name: performance
description: "MUST be used when fixing Flows app performance — re-renders, query patterns, pagination, unbounded fetches, LLM-over-query-results, bundles, memory leaks. Measure before and after. Triggers: performance, slow, laggy, optimize, re-render, bundle size, CDF query, virtualize, chat completions, LLM cost."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[file, component, or area to optimize — e.g. 'src/components/AssetTable.tsx']"
---

# Performance Fix

Systematically find and fix performance issues in **$ARGUMENTS** (or the whole app if no argument is given). Always measure first — never optimize blindly.

---

## Step 1 — Measure baseline before touching anything

Run the production build and capture metrics before making any changes:

```bash
pnpm run build
pnpm run preview
```

Open the app in Chrome and capture:
- **Lighthouse score** (Performance tab → Run audit)
- **React Profiler** (React DevTools → Profiler → Record an interaction)
  - Note the components with the longest render times and highest render counts

Record baseline numbers. Every fix must be measured against these.

---

## Step 2 — Find and fix unnecessary re-renders

Read the component tree (start from `src/App.tsx`) and search for these patterns:

```bash
grep -rn --include="*.tsx" \
  -E "value=\{\{|onClick=\{\(\)" src/
```

For each instance found, **apply the fix directly**:

**Inline object/array creation in JSX → wrap with `useMemo`:**
```tsx
// BAD — new object on every render causes children to re-render
<Chart options={{ color: "red" }} />

// FIX — wrap with useMemo
const chartOptions = useMemo(() => ({ color: "red" }), []);
<Chart options={chartOptions} />
```

**Event handlers recreated on every render → wrap with `useCallback`:**
```tsx
// BAD
<Button onClick={() => doSomething(id)} />

// FIX — wrap with useCallback
const handleClick = useCallback(() => doSomething(id), [id]);
<Button onClick={handleClick} />
```

**Context that changes on every render → memoize the context value:**
```tsx
// BAD — new object reference every render
<MyContext.Provider value={{ user, sdk }}>

// FIX — memoize the context value
const ctxValue = useMemo(() => ({ user, sdk }), [user, sdk]);
<MyContext.Provider value={ctxValue}>
```

Apply `React.memo` to pure presentational components that receive stable props. Do NOT wrap every component — only those confirmed to re-render unnecessarily via the Profiler.

---

## Step 3 — Find and fix DMS query patterns

For **read-heavy** workloads, prefer APIs that hit the **search/Elasticsearch path** (`query` or `search` on instances) rather than `list` paths that stress **Postgres**.

```bash
# Find all DMS instance API calls
grep -rn --include="*.ts" --include="*.tsx" -E "instances\.(list|search|query|aggregate|retrieve)" src/

# Find direct SDK calls to other CDF resources
grep -rn --include="*.ts" --include="*.tsx" -E "\.(assets|timeseries|events|files|sequences|relationships)\.(list|search|retrieve)" src/
```

For each `instances.list` call in a read-heavy path (e.g. populating a table, dropdown, or search results), **rewrite it to use `instances.query`** with the equivalent filter. Preserve the existing filter logic but express it in the query API format:

```ts
// BAD — instances.list hits Postgres, expensive for read-heavy UI
const result = await client.instances.list({
  instanceType: "node",
  filter: { equals: { property: ["node", "space"], value: "my-space" } },
  limit: 100,
});

// FIX — rewrite to instances.query which hits Elasticsearch
const result = await client.instances.query({
  with: {
    nodes: {
      nodes: {
        filter: { equals: { property: ["node", "space"], value: "my-space" } },
      },
      limit: 100,
    },
  },
  select: {
    nodes: {},
  },
});
```

| API used | When it's correct | When to rewrite |
|----------|-------------------|-----------------|
| `instances.query` | Read with filters that map to Elasticsearch (text, equals, range) | — |
| `instances.search` | Full-text or fuzzy search | — |
| `instances.list` | Writing, syncing, or need for semantics not available on query/search | Rewrite to `instances.query` if used for read-heavy UI display |
| `instances.retrieve` | Fetching by known external IDs | — |
| `instances.aggregate` | Counts, histograms | — |

For deeper rationale on search vs relational paths, cardinality, and materialization tradeoffs, consult the `semantic-knowledge/` directory if available in the workspace.

### Hard gate — LLM over query results

```bash
grep -rn --include="*.ts" --include="*.tsx" -E "chat\.completions|agents/chat|useAtlasChat|openai|anthropic" src/
```

Do not map completions over DMS rows. Fix: one `sendAgentMessage` or agent resource (`integrate-fusion-agent`). If per-item completions remain: **5** / ceiling **50**, cache by `space:externalId:lastUpdatedTime`, user-initiated only.

---

## Step 4 — Find and fix client-side filtering (move to server-side)

Filters, limits, and projections must be applied **in the API request** — not by downloading large result sets and filtering in the browser.

```bash
# Find client-side filtering after data fetch (common anti-pattern)
grep -rn --include="*.ts" --include="*.tsx" -B 5 "\.filter(" src/ | grep -B 5 "data\|items\|result\|response\|nodes"

# Find .map() or .reduce() on full datasets that suggest client-side processing
grep -rn --include="*.ts" --include="*.tsx" -E "\.(map|reduce|find|some|every)\(" src/hooks/ src/services/ src/api/
```

For each client-side filter pattern, **move the filter logic into the SDK call's `filter` parameter and remove the `.filter()` call**:

```ts
// BAD — fetches all nodes then filters client-side
const result = await client.instances.query({ ... });
const activeNodes = result.items.nodes.filter(n => n.properties.status === "active");

// FIX — move filter into the API request, remove client-side .filter()
const result = await client.instances.query({
  with: {
    nodes: {
      nodes: {
        filter: {
          and: [
            existingFilters,
            { equals: { property: ["mySpace", "myView/v1", "status"], value: "active" } },
          ],
        },
      },
      limit: 100,
    },
  },
  select: { nodes: {} },
});
const activeNodes = result.items.nodes; // no client-side filter needed
```

| Issue | Fix |
|-------|-----|
| `.filter()` after SDK call on full result set | Move the filter into the API request's `filter` parameter and delete the `.filter()` |
| No `properties` selection in DMS query | Add a `sources` or `properties` parameter to fetch only needed fields |
| Fetching all items then rendering a subset | Add `limit` and `filter` to the API call to fetch only what's displayed |
| Client-side text search on fetched array | Replace with the SDK's `search` endpoint |

**Hard rule:** If the API supports a filter for the criterion being applied client-side, **move it server-side now**. Client-side filtering is acceptable only for trivial local state (e.g. filtering a cached list of 10 user preferences). If the API does not support the exact filter, add a code comment explaining why client-side filtering is necessary.

---

## Step 5 — Find and fix CDF data fetching and pagination

Read all CDF SDK calls (search for `sdk.`, `client.`, `useQuery`, `useCogniteClient`).

```bash
# Find pagination patterns
grep -rn --include="*.ts" --include="*.tsx" -E "(nextCursor|cursor|hasNextPage|fetchNextPage|offset|skip|page)" src/

# Find "fetch all" loops
grep -rn --include="*.ts" --include="*.tsx" -B 3 -A 3 "while.*cursor\|while.*hasMore\|while.*nextPage" src/
```

For each call, find the issue and **apply the fix**:

| Issue | Fix to apply |
|-------|-------------|
| No `limit` set | **Add `limit: 100`** (or the actual page size needed) to the SDK call |
| Fetching all properties | **Add a `properties` filter** to select only required fields |
| Fetching on every render | **Move inside `useQuery`/`useMemo`** with a stable dependency array |
| Sequential requests that could be parallel | **Rewrite to `Promise.all`** or batched SDK methods |
| Missing `limit` parameter | **Add explicit `limit`** matching the UI's page size (e.g. 25, 50, 100) |
| Offset-based pagination for large datasets | **Replace with cursor-based pagination** using `nextCursor` from the response |
| "Fetch all" loop (exhausts cursors up front) | **Replace with on-demand pagination** using TanStack Query's `useInfiniteQuery` |

**Fixing fetch-all loops** — replace the while loop with `useInfiniteQuery`:

```ts
// BAD — fetches ALL pages before rendering
let allItems = [];
let cursor = undefined;
while (true) {
  const result = await client.instances.list({ limit: 1000, cursor });
  allItems.push(...result.items);
  if (!result.nextCursor) break;
  cursor = result.nextCursor;
}

// FIX — paginate on demand with useInfiniteQuery
const { data, fetchNextPage, hasNextPage } = useInfiniteQuery({
  queryKey: ["instances", filters],
  queryFn: ({ pageParam }) =>
    client.instances.list({ limit: 100, cursor: pageParam, ...filters }),
  getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
  staleTime: 30_000,
});
```

**Fixing offset-based pagination** — switch to cursor-based:

```ts
// BAD — offset pagination degrades at scale
const result = await client.instances.list({ limit: 100, offset: page * 100 });

// FIX — cursor-based pagination
const result = await client.instances.list({ limit: 100, cursor: nextCursor });
```

---

## Step 6 — Find and fix excessive API call rates

```bash
# Find search/filter inputs that trigger queries
grep -rn --include="*.tsx" --include="*.ts" -E "onChange|onInput|onSearch|onFilter" src/ | grep -i "search\|filter\|query"

# Find debounce usage
grep -rn --include="*.ts" --include="*.tsx" -i -E "debounce|useDebouncedValue|useDebounce" src/

# Find polling/interval patterns
grep -rn --include="*.ts" --include="*.tsx" -E "setInterval|refetchInterval|pollingInterval|refetchOnWindowFocus" src/

# Find useQuery options that control refetch behavior
grep -rn --include="*.ts" --include="*.tsx" -E "staleTime|cacheTime|gcTime|refetchOnMount|refetchOnWindowFocus" src/
```

For each issue found, **apply the fix**:

**Search inputs that fire on every keystroke → add debounce with 300ms delay:**
```tsx
// BAD — fires API call on every keystroke
const [search, setSearch] = useState("");
const { data } = useQuery({ queryKey: ["search", search], queryFn: () => api.search(search) });

// FIX — create or use a useDebouncedValue hook with 300ms delay
function useDebouncedValue<T>(value: T, delay = 300): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debounced;
}

const [search, setSearch] = useState("");
const debouncedSearch = useDebouncedValue(search, 300);
const { data } = useQuery({
  queryKey: ["search", debouncedSearch],
  queryFn: () => api.search(debouncedSearch),
  enabled: debouncedSearch.length > 0,
});
```

**useQuery calls without staleTime → add appropriate staleTime:**
```ts
// BAD — refetches on every mount/focus
useQuery({ queryKey: ["data"], queryFn: fetchData });

// FIX — add staleTime to prevent unnecessary refetches
useQuery({ queryKey: ["data"], queryFn: fetchData, staleTime: 30_000 });
```

**Duplicate parallel identical requests → lift the query to a shared hook:**
```ts
// BAD — multiple components each call the same query independently
// ComponentA.tsx: useQuery({ queryKey: ["assets"], queryFn: fetchAssets });
// ComponentB.tsx: useQuery({ queryKey: ["assets"], queryFn: fetchAssets });

// FIX — create a shared hook, import it from both components
// hooks/useAssets.ts
export function useAssets() {
  return useQuery({ queryKey: ["assets"], queryFn: fetchAssets, staleTime: 30_000 });
}
```

| Issue | Fix to apply |
|-------|-------------|
| Search input fires query on every keystroke | **Add `useDebouncedValue` hook** with 300ms delay |
| Polling with no backoff or very short interval | **Set interval to ≥30s** with exponential backoff on errors |
| Re-fetching on every render (no caching) | **Add `staleTime: 30_000`** (or appropriate) to useQuery options |
| `refetchOnWindowFocus: true` for expensive queries | **Set `refetchOnWindowFocus: false`** or use a longer stale time |
| Duplicate parallel identical requests | **Lift the query to a shared hook** and import from both components |
| Multiple components triggering the same fetch | **Extract to a shared hook** in `hooks/` directory |

---

## Step 7 — Find and fix large un-virtualized lists

Search for lists that render more than ~50 items:
```bash
grep -rn --include="*.tsx" -E "\.(map|forEach)\(" src/
```

For any list where the data source could exceed 50 items, **replace the plain `.map()` render with a virtualized list**. Install `@tanstack/react-virtual` if not present:

```bash
pnpm add @tanstack/react-virtual
```

**Apply the virtualizer pattern directly:**

```tsx
// BAD — renders all items in the DOM
<div>
  {items.map((item) => (
    <div key={item.id}>{item.name}</div>
  ))}
</div>

// FIX — replace with virtualized list
import { useVirtualizer } from "@tanstack/react-virtual";

const parentRef = useRef<HTMLDivElement>(null);
const rowVirtualizer = useVirtualizer({
  count: items.length,
  getScrollElement: () => parentRef.current,
  estimateSize: () => 48,
});

return (
  <div ref={parentRef} style={{ height: "600px", overflow: "auto" }}>
    <div style={{ height: rowVirtualizer.getTotalSize(), position: "relative" }}>
      {rowVirtualizer.getVirtualItems().map((virtualRow) => (
        <div
          key={virtualRow.key}
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            width: "100%",
            height: `${virtualRow.size}px`,
            transform: `translateY(${virtualRow.start}px)`,
          }}
        >
          {items[virtualRow.index].name}
        </div>
      ))}
    </div>
  </div>
);
```

---

## Step 8 — Find and fix missing code splitting

Read the router setup and identify routes that are imported statically but not shown on the landing page.

**For each statically imported heavy page, convert to lazy import with `React.lazy()` and `Suspense`:**

```tsx
// BAD — statically imported, loaded in initial bundle
import { ReportPage } from "./pages/ReportPage";

// FIX — convert to lazy import
import { lazy, Suspense } from "react";
const ReportPage = lazy(() => import("./pages/ReportPage"));

// In the route — wrap with Suspense
<Suspense fallback={<PageSkeleton />}>
  <ReportPage />
</Suspense>
```

Similarly, large third-party components (chart libraries, PDF viewers, map renderers) should be dynamically imported inside the component that needs them, not at the module level. **Apply the transformation directly** to each heavy import found.

---

## Step 9 — Analyse and fix bundle size

```bash
# Install if not already present, then run
pnpm add -D rollup-plugin-visualizer
```

Add to `vite.config.ts` temporarily:
```ts
import { visualizer } from "rollup-plugin-visualizer";

export default defineConfig({
  plugins: [
    react(),
    visualizer({ open: true, gzipSize: true, brotliSize: true }),
  ],
});
```

Run `pnpm run build` and inspect the treemap. For any chunk > 100 KB (gzipped) that is not a necessary initial dependency, **apply the fix**:

| Issue | Fix to apply |
|-------|-------------|
| `lodash` (full bundle) | **Replace with `lodash-es`** individual imports or native equivalents (e.g., `Array.prototype.map`, `Object.entries`, `structuredClone`) |
| `moment` | **Replace with `date-fns`** or native `Intl.DateTimeFormat` |
| Chart libraries not tree-shaken | **Switch to named imports** (e.g., `import { LineChart } from "echarts/charts"`) |
| Large library used in one place | **Dynamically import it** with `React.lazy` or inline `import()` |

```ts
// BAD
import _ from "lodash";
const sorted = _.sortBy(items, "name");

// FIX — use lodash-es or native
import sortBy from "lodash-es/sortBy";
const sorted = sortBy(items, "name");
// OR native:
const sorted = [...items].sort((a, b) => a.name.localeCompare(b.name));
```

```ts
// BAD
import moment from "moment";
const formatted = moment(date).format("YYYY-MM-DD");

// FIX — use date-fns
import { format } from "date-fns";
const formatted = format(date, "yyyy-MM-dd");
```

**After analysis, remove the visualizer plugin** from `vite.config.ts` and uninstall it:
```bash
pnpm remove rollup-plugin-visualizer
```

---

## Step 10 — Find and fix memory leaks

Search for `useEffect` hooks that set up subscriptions, timers, or event listeners without cleanup:

```bash
grep -rn --include="*.tsx" --include="*.ts" -A 10 "useEffect" src/
```

For every `useEffect` that calls `addEventListener`, `setInterval`, `setTimeout`, `subscribe`, or sets up a CDF streaming connection, **add the missing cleanup function**:

**Fetch without abort → add AbortController:**
```ts
// BAD — no cleanup, fetch continues after unmount
useEffect(() => {
  fetchData(id);
}, [id]);

// FIX — add AbortController for cleanup
useEffect(() => {
  const controller = new AbortController();
  fetchData(id, controller.signal);
  return () => controller.abort();
}, [id]);
```

**Timer without cleanup → add clearInterval/clearTimeout:**
```ts
// BAD — interval keeps running after unmount
useEffect(() => {
  const id = setInterval(() => poll(), 5000);
}, []);

// FIX — add clearInterval cleanup
useEffect(() => {
  const id = setInterval(() => poll(), 5000);
  return () => clearInterval(id);
}, []);
```

**Event listener without cleanup → add removeEventListener:**
```ts
// BAD — listener accumulates on each render
useEffect(() => {
  window.addEventListener("resize", handleResize);
}, []);

// FIX — add removeEventListener cleanup
useEffect(() => {
  window.addEventListener("resize", handleResize);
  return () => window.removeEventListener("resize", handleResize);
}, []);
```

---

## Step 11 — Measure after and report the delta

Re-run the same Lighthouse audit and React Profiler session from Step 1. Report the delta and list every file changed:

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Lighthouse Performance | 72 | 91 | +19 |
| Largest Contentful Paint | 3.2 s | 1.8 s | −1.4 s |
| Total Blocking Time | 420 ms | 80 ms | −340 ms |
| Bundle size (gzipped) | 410 KB | 290 KB | −120 KB |
| `AssetTable` render count (on filter change) | 8 | 2 | −6 |

If a step produced no improvement, state that explicitly. Do not fabricate numbers.

---

## Done

List every file changed with the absolute path and a one-line explanation of what was fixed. If further gains require server-side or infrastructure changes (e.g., CDF response caching, CDN configuration), note them separately as out-of-scope recommendations.
