# Operations Summarizer — Flows code review

This document is the platform review for Operations Summarizer, conducted as part of the Cognite Flows app certification process.

## Path to approval

This review found **0 must-fix item(s)** that block approval. Once the must-fix items are addressed, re-run `flows-code-review`.

No must-fix items are open. The next certification step is `flows-design-review`.

### Reviewed commit

`f54ea2820ff0caf578dac69ef57e13823d85526d`

Review target is the working tree of `modules/tools/apps/operations_summarizer` at that commit (local uncommitted app files included). Skills were pulled from `cognitedata/builder-skills` `main` into this app before scoring.

## Checks performed

`flows-review-checks` Step 1 hunts (coverage config, correctness, CDF Raw, DMS, `code-quality` searches, component size, unused files), Step 2 package audit, Step 3 Vitest coverage, Step 4 scores, Step 5 categorization. `code-quality` fix steps were not applied.

## Coverage scope

- Runner: Vitest 4.1.12, v8, `npx vitest run --coverage`.
- Include: `src/**/*.{ts,tsx}`.
- Exclude: tests, `vite-env.d.ts`, `src/main.tsx`, plus `src/test-utils/**` and `src/__mocks__/**`.
- Test exclude: `node_modules`, `dist`, `.agents`, `.claude` only.
- Result: 92 files passed, 731 tests passed, 1 unhandled worker error (`src/utils/sanitize.test.ts` did not start). Exit code 1.
- Printed coverage on the include set: statements 92.76% (3182/3430), branches 85.52% (2079/2431), functions 92.9% (759/817), lines **94.17%** (3021/3208).
- `src/utils/sanitize.ts` is inside the include set and measured at 0% lines because its test worker timed out. That does not hide the file, and the suite line total stays above 80%.

## Scores

| Area | Criterion | Score | Notes |
| ---- | --------- | ----- | ----- |
| User & customer | 1.1 Known bugs | 5/5 | `src/App.tsx` wraps the tree in `react-error-boundary`. List and detail screens expose loading, error, and empty states. `useEffect` timers and fetches abort or clear. No TODO/FIXME on app paths. |
| User & customer | 1.3 Packages | 4/5 | Lockfile install audited. No high or critical CVE, no deprecated direct dependency, no production major lag. Low transitive `katex` advisory via `@cognite/aura`, and several packages are a patch or minor behind latest. |
| User & customer | 1.4 Tests & coverage | 4/5 | Honest line coverage is 94.17% of `src/**` except tests, `main.tsx`, `vite-env.d.ts`, and test-harness folders. One worker failed to start `sanitize.test.ts` in this run. |
| User & customer | 1.5 Dead code | 4/5 | Lint and `tsc` are clean. No production `any`. No unreachable route. `useDashboardStats` is only referenced by its test. |
| User & customer | 1.6 Patterns & testability | 4/5 | CDF hooks take the client from context. Summaries and tests pages use ViewModel hooks. Services implement interfaces. `vi.mock` is still common in tests, and many component filenames are PascalCase. |
| Cognite services | 2.1 DMS query patterns | 4/5 | Operation-summary pages use `instances.query` with a property projection (`src/utils/summaryListQuery.ts`). Test reports and the stats walk still use `instances.list` where the full node is required. |
| Cognite services | 2.2 Server-side filter | 4/5 | Space, external-id prefix, and date range are server filters. Content type is a bounded client scan (`maxExtraFetches` 10) for legacy rows. |
| Cognite services | 2.3 Limits & pages | 3/5 | UI lists use `summaryListPageSize` and cursors. Opening Tests still walks every test-report page in `computeTestReportStatsStreaming` until the cursor ends. |
| Cognite services | 2.4 Call rate | 4/5 | React Query `staleTime` is 2 minutes and SDK calls go through `QueuedTaskRunner`. The stats walk is sequential, not a parallel storm, but it repeats a full scan when the query is stale. |
| Cognite services | 2.5 429 backoff | 5/5 | `getCdfTaskRunner` caps concurrency. `cdfRetry` honors `Retry-After` and uses exponential backoff with jitter for user retry. The SDK retries HTTP 429s on the request path. |
| Cognite services | 2.6 CDF Raw | N/A | No `client.raw`, `listRows`, `insertRows`, or `retrieveRow` in `src/`. |
| Brand | 3.1 Aura | 4/5 | Buttons, inputs, dialogs, cards, and loaders come from `@cognite/aura/components`. Summary and test tables are custom markup using the same tokens. Labeled inputs and icon buttons that were checked have `aria-label` or `aria-labelledby`. |

## Must Fix

None.

## Should Fix

1. **Test history stats read every test-report page.** `src/utils/testReportStatsStreaming.ts:43` loops `instances.list` with `getInstanceQueryLimit()` until `nextCursor` is empty. `useDefaultTestsPageCdfBundle` calls that on the Tests page, so opening Tests downloads the whole report set to compute header counts. Replace the scan with a bounded aggregate or a stored summary so the page does not grow with history.

## Nice Fix

1. Remove or mount `useDashboardStats` (`src/hooks/useDashboardStats.ts:36`). Nothing in the UI imports it.
2. When legacy summary rows no longer need description heuristics, filter content type in the DMS request (`src/utils/summaryListQuery.ts:128`).
3. Prefer `instances.query` for test-report lists where a projection is enough (`src/hooks/useSummariesCdf.ts:180`).
4. Split the largest presentational files (Summary table, testing info dialog, create form, navigation) when the next edit touches them. They render ViewModel data; they do not fetch it.
5. Add a one-line comment on `vi.mock("@cognite/app-sdk/react")` factories that stub the Fusion host.
6. Drop `console.info` at `src/hooks/useAgentsCdf.ts:125` or route it through the app logger.
7. Drop `src/test-utils/**` and `src/__mocks__/**` from `coverage.exclude`, or move them outside `src/`, so the exclude list matches the allowed set.
8. Re-run `npx vitest run --coverage` so `src/utils/sanitize.test.ts` actually starts. This run timed out a worker while other commands were in flight; `sanitizeHtml` is the XSS boundary.
9. Bump in-range packages (`@cognite/sdk`, `@cognite/aura`, `lucide-react`, pinned `pdfjs-dist`) and track the low `katex` advisory until Aura ships a patched line.
10. New component files can use kebab-case (`summary-table.tsx`) to match `code-quality`. Existing PascalCase names are consistent.

## Summary

- Must Fix open: 0
- Should Fix open: 1
- Nice Fix open: 10
