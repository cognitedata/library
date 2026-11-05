---
name: correctness-and-error-handling
description: "MUST be used whenever fixing correctness and error handling issues in a Flows app. This skill finds AND fixes bugs, missing error states, unhandled rejections, and edge-case failures — it does not just report them. Triggers: correctness, error handling, bug fix, edge case, crash, unhandled, null, undefined, empty state, loading state, error boundary, try catch, async error, useEffect cleanup, type guard, runtime error, robustness."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[file or directory to fix, or leave blank to fix the whole app]"
---

# Correctness & Error Handling Fix

Find and fix correctness issues and missing error handling in **$ARGUMENTS** (or the whole app if no argument is given). Work through every step below. Each step searches for problems and then **fixes them in place**. Only report issues that cannot be auto-fixed.

---

## Step 1 — Map data flows and fix known defects

Read these files before checking anything:

- `src/main.tsx` / `src/App.tsx` — top-level error boundaries and auth flow
- All files matching `**/hooks/*.ts`, `**/contexts/*.tsx` — shared async state
- All files matching `**/api/*.ts`, `**/services/*.ts` — CDF SDK call sites

For each async data source, note:
- What happens when the request fails (network error, CDF 403, timeout)?
- What does the UI show while loading?
- What does the UI show if the result is empty?

### Find and fix known defects in critical paths

```bash
# Find TODO/FIXME/HACK in critical code paths (not test files)
grep -rn --include="*.ts" --include="*.tsx" -E "(TODO|FIXME|HACK|XXX):" src/ | grep -v ".test." | grep -v ".spec."

# Find "fix" or "broken" or "workaround" markers
grep -rn --include="*.ts" --include="*.tsx" -i -E "(TODO.*fix|workaround|broken|known.?bug|temporary.?hack)" src/
```

For each match in a critical path (data fetching, rendering, auth, navigation):

1. **Read the surrounding code** to understand the incomplete/broken behavior.
2. **Fix the underlying issue** — implement the missing logic, correct the broken behavior, or add proper error handling.
3. If the fix requires significant architectural changes beyond this skill's scope, **replace the TODO with a safe failure mode**: graceful error handling, a sensible fallback value, or an explicit user-facing message explaining degraded functionality.
4. **Remove the TODO/FIXME/HACK comment** after fixing. The code should speak for itself.

Do not leave TODOs in critical paths. Every one must be resolved or converted to a safe fallback.

---

## Step 2 — Add top-level error boundary

Every Flows app must have at least one React Error Boundary wrapping the main content so that an unexpected render-time exception shows a user-friendly message instead of a blank screen.

```bash
grep -rn --include="*.tsx" --include="*.ts" -E "ErrorBoundary|componentDidCatch|getDerivedStateFromError" src/
```

If no error boundary exists, **create the ErrorFallback component and add the ErrorBoundary wrapper** to `App.tsx`. Install `react-error-boundary` if not present:

```bash
pnpm add react-error-boundary
```

Then add to `App.tsx`:

```tsx
import { ErrorBoundary } from "react-error-boundary";

function ErrorFallback({ error }: { error: Error }) {
  return (
    <div role="alert" className="p-8 text-center">
      <p className="text-lg font-semibold">Something went wrong</p>
      <pre className="mt-2 text-sm text-muted-foreground">{error.message}</pre>
    </div>
  );
}

// Wrap the main content:
<ErrorBoundary FallbackComponent={ErrorFallback}>
  <MainContent />
</ErrorBoundary>
```

Write the updated `App.tsx` with the ErrorBoundary in place. Do not just suggest it — make the edit.

---

## Step 3 — Wrap unhandled async functions in try/catch

Search for every `async` function and `Promise` chain that does not have error handling:

```bash
# Find async functions
grep -rn --include="*.ts" --include="*.tsx" -E "async\s+function|async\s+\(" src/

# Find .then() without .catch()
grep -rn --include="*.ts" --include="*.tsx" -E "\.then\(" src/ | grep -v "\.catch\("
```

**Fix each one:**

- For bare `async` functions that lack try/catch: **wrap the function body** in try/catch. Log the error with context and re-throw so callers/query layers can handle it:

```ts
async function fetchAssets(sdk: CogniteClient) {
  try {
    const result = await sdk.assets.list({ limit: 100 });
    return result.items;
  } catch (error) {
    console.error("Failed to fetch assets:", error);
    throw error;
  }
}
```

- For `.then()` without `.catch()`: **add `.catch()`** to the chain:

```ts
somePromise.then(handleResult).catch((error) => {
  console.error("Operation failed:", error);
});
```

- For TanStack Query consumers (`useQuery`/`useMutation`) missing `isError` handling: **add the error check and error UI** to the component:

```tsx
const { data, isLoading, isError, error } = useQuery({
  queryKey: ["assets"],
  queryFn: () => fetchAssets(sdk),
});

if (isError) return <ErrorMessage error={error} />;
```

Read each file, make the edit, and write it back.

---

## Step 4 — Add missing loading, error, and empty states to components

For each component that fetches data, it must have three distinct UI states:

| State | Required UI |
|-------|-------------|
| Loading | Spinner, skeleton, or loading indicator |
| Error | User-readable message (not a raw error object or blank space) |
| Empty | "No results" / "Nothing here yet" message (not a blank list) |

Search for components that render data without checking loading state:

```bash
grep -rn --include="*.tsx" -E "\.(map|filter|find)\(" src/ | grep -v "isLoading\|isPending\|skeleton\|Skeleton"
```

For each hit, read the component and **add the missing states directly**:

- **Missing loading state** — add before the data render:
```tsx
if (isLoading) {
  return <div className="flex items-center justify-center p-8"><Spinner /></div>;
}
```

- **Missing error state** — add after the loading check:
```tsx
if (isError) {
  return (
    <div role="alert" className="p-4 text-center text-destructive">
      <p>Failed to load data. Please try again.</p>
    </div>
  );
}
```

- **Missing empty state** — add after the error check, before the `.map()`:
```tsx
if (!data || data.length === 0) {
  return (
    <div className="p-8 text-center text-muted-foreground">
      <p>No results found.</p>
    </div>
  );
}
```

Insert these checks in the correct order (loading, then error, then empty) above the existing data render. Write each fixed file.

---

## Step 5 — Add type narrowing for external data

External data (CDF responses, URL params, `localStorage`, `JSON.parse`) must be validated before use. TypeScript types alone are not runtime guarantees.

```bash
# Find JSON.parse without validation
grep -rn --include="*.ts" --include="*.tsx" -E "JSON\.parse\(" src/

# Find localStorage reads
grep -rn --include="*.ts" --include="*.tsx" -E "localStorage\.(get|set)Item" src/

# Find useSearchParams usage
grep -rn --include="*.ts" --include="*.tsx" -E "useSearchParams|searchParams\.get" src/
```

**Fix each one:**

- **`JSON.parse(x) as T`** — replace with Zod safeParse:
```ts
import { z } from "zod";

const MySchema = z.object({ /* fields */ });
const parseResult = MySchema.safeParse(JSON.parse(raw));
if (!parseResult.success) {
  console.warn("Invalid stored data, using defaults:", parseResult.error);
  return defaultValue;
}
const validated = parseResult.data;
```

- **`searchParams.get("id")`** without null check — add nullish fallback:
```ts
const id = searchParams.get("id") ?? defaultId;
```

- **`localStorage.getItem(key)`** used directly — add type guard and fallback:
```ts
const raw = localStorage.getItem(key);
if (raw === null) return defaultValue;
try {
  const parsed = JSON.parse(raw);
  // validate parsed shape
  return isValidShape(parsed) ? parsed : defaultValue;
} catch {
  return defaultValue;
}
```

Do not cast external data with `as MyType` — that bypasses runtime safety. Read, fix, and write each file.

---

## Step 6 — Fix null, undefined, and unsafe array access

Read every component that accesses properties of data returned from CDF or passed via props.

```bash
grep -rn --include="*.tsx" --include="*.ts" -E "\w+\[0\]\." src/
```

**Fix each unsafe pattern found:**

- **Unsafe nested property access** — add optional chaining and nullish coalescing:
```tsx
// Before: asset.properties.space.Asset.name
// After:
const name = asset.properties?.["my-space"]?.["Asset"]?.name ?? "Unknown";
```

- **Unguarded `.map()` on possibly-undefined array** — add nullish fallback:
```tsx
// Before: items.map(renderItem)
// After:
(items ?? []).map(renderItem)
```

- **Unsafe array index access** — use `.at()` with optional chaining:
```tsx
// Before: items[0].name
// After:
const first = items.at(0)?.name ?? "—";
```

Read each file with a match, apply the fix, and write it back.

---

## Step 7 — Add useEffect cleanup functions

Every `useEffect` that sets up a subscription, timer, event listener, or async operation that can outlive the component must return a cleanup function.

```bash
grep -rn --include="*.tsx" --include="*.ts" -B 2 -A 15 "useEffect" src/
```

For each `useEffect`, check whether cleanup is needed and **add the cleanup function** if missing:

| Pattern | Fix to add |
|---------|-----------|
| `addEventListener` | Add `return () => removeEventListener(...)` |
| `setInterval` / `setTimeout` | Add `return () => clearInterval(id)` / `clearTimeout(id)` |
| CDF streaming / SSE | Add `return () => stream.close()` |
| `fetch` / CDF SDK call | Add AbortController: `const controller = new AbortController()` at the top, pass `controller.signal` to fetch, add `return () => controller.abort()`, and guard state updates with `if (!controller.signal.aborted)` |
| Zustand / event emitter subscription | Add `return () => unsubscribe()` |

Reference pattern for async effects:

```ts
useEffect(() => {
  const controller = new AbortController();

  async function load() {
    try {
      const data = await fetchWithSignal(controller.signal);
      if (!controller.signal.aborted) setState(data);
    } catch (err) {
      if (err instanceof Error && err.name !== "AbortError") {
        setError(err);
      }
    }
  }

  load();
  return () => controller.abort();
}, [id]);
```

Read each effect, add the missing cleanup, and write the file.

---

## Step 8 — Add edge case guards

For each feature, check and **add guards** for:

- **Empty data**: If zero-item lists are not handled, add an empty state check before rendering.
- **Single item**: If list rendering has off-by-one bugs with a single entry, fix the logic.
- **Maximum data / pagination**: If CDF returns the full `limit` and there are more pages, ensure pagination is communicated to the user. Add a "Load more" or pagination indicator if missing.
- **Concurrent requests / stale results**: If the user can trigger a new request before the previous completes, add stale request cancellation (AbortController or a request ID check).
- **Network offline**: If the app silently fails when offline, add a meaningful error message.

For Atlas tool `execute` functions, **add argument validation** at the top of every execute function:

```ts
execute: async (args) => {
  if (!args.assetId || typeof args.assetId !== "string") {
    return { output: "Missing or invalid assetId", details: null };
  }
  // ... safe to proceed
}
```

Search for `execute` functions, read each one, add the validation, and write the file.

---

## Step 9 — Report remaining findings

Produce a structured report covering:

1. **What was fixed in each step** — summarize the changes made (files edited, patterns fixed).
2. **Remaining issues** — only list issues that could not be auto-fixed (e.g., require architectural changes, need product decisions, or are outside the scope of this skill).

| Severity | File | Line | Issue | Status |
|----------|------|------|-------|--------|
| HIGH | `src/hooks/useAssets.ts` | 34 | Unhandled promise rejection | FIXED — wrapped in try/catch |
| MEDIUM | `src/components/AssetList.tsx` | 12 | No empty state | FIXED — added empty state check |
| MEDIUM | `src/auth/flow.ts` | 45 | Auth error handling needs product decision | UNFIXED — requires team input |

If no issues are found in a step, state "No issues found" for that step. Do not skip steps silently.

---

## Done

Summarize what was fixed by severity. Flag any remaining HIGH issues that could cause data loss, crashes in production, or misleading UI states, and list them first for immediate attention.
