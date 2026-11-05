# Findings: Operations Summarizer

Checks loaded from `cognitedata/builder-skills` (`flows-review-checks` and `code-quality`) before scoring. `code-quality` searches were run. Its fix steps were not applied.

## Config inspected

- Coverage config file(s): `vitest.config.ts` (`coverage` and `test` blocks). `vite.config.ts` has no `test` or `coverage` block. No Jest config.
- Production paths excluded from coverage: none. Extra excludes under `src/` are `src/test-utils/**` and `src/__mocks__/**` (test harness, not pages/hooks/components/services). Allowed excludes also present: `**/*.{test,spec}.{ts,tsx}`, `**/vite-env.d.ts`, `src/main.tsx`.
- Tests excluded from the test run: `**/node_modules/**`, `**/dist/**`, `**/.agents/**`, `**/.claude/**` only. No `src/` test files are excluded.

`coverage.include` is `src/**/*.{ts,tsx}`, so production modules are measured.

## Searches

| Check | Hits (file:line or none) |
| ----- | ------------------------ |
| ErrorBoundary | `src/App.tsx:10`, `src/App.tsx:40`, `src/App.tsx:70` (`react-error-boundary`) |
| TODO/FIXME/HACK/XXX | none |
| useEffect | Production: `src/hooks/useSummariesPageModel.ts:130` (state sync, no subscription), `src/features/dashboard-agent/useAgentLoader.ts:51`, `src/features/dashboard-agent/useAgentLoader.ts:63` (state sync), `src/hooks/useSummaryGeneration.ts:102` (interval cleared on cleanup), `src/components/layout/Navigation.tsx:41` (class toggle, no listener), `src/hooks/usePdfBlob.ts:62` (abort + cancelled flag), `src/pages/SummaryDetailModal.tsx:53` (rAF cancelled), `src/contexts/operations-summarizer/AgentBootstrap.tsx:26` (cancelled flag), `src/hooks/useEnhancedRefresh.ts:42`, `src/hooks/useEnhancedRefresh.ts:54` (timeouts cleared), `src/hooks/testsPageModel/useTestsPageFilters.ts:84` (no subscription), `src/hooks/useSummariesCdf.ts:130` (reset pagination). Test-only effects omitted. |
| useQuery / useMutation | `src/hooks/useSavedPodcast.ts:40`, `src/contexts/AppConfigContext.tsx:36`, `src/hooks/useAgentsCdf.ts:45`, `src/hooks/useAgentsCdf.ts:66`, `src/hooks/useAgentsCdf.ts:89`, `src/hooks/useSummariesCdf.ts:138`, `src/hooks/useSummariesCdf.ts:284`, `src/hooks/useSummariesCdf.ts:319`, `src/hooks/useSummariesCdf.ts:344`, `src/hooks/useSummariesCdf.ts:367`, `src/hooks/useSummariesCdf.ts:399`, `src/hooks/useFileContent.ts:37`. Pages do not call `useQuery` / `useMutation`. |
| coverage/test exclude | `vitest.config.ts:23-40`. See Config inspected. |
| CDF Raw | none |
| instances.list/query/search | Production: `src/services/appConfigService.ts:107` (`instances.retrieve`), `src/utils/summaryListQuery.ts:306` (`instances.query`), `src/hooks/useSummariesCdf.ts:163` (`query`), `src/hooks/useSummariesCdf.ts:180` (`list`, test-report variant), `src/hooks/useSummariesCdf.ts:298` (`query`), `src/hooks/useSummariesCdf.ts:303` (`list`), `src/utils/testReportStatsStreaming.ts:50` (`list`). |
| QueuedTaskRunner / 429 | `src/utils/semaphore.ts:22`, `src/utils/semaphore.ts:110` (`getCdfTaskRunner`), `src/utils/cdfRequest.ts:12`, `src/utils/cdfRetry.ts:23` and `src/utils/cdfRetry.ts:41` (Retry-After + exponential backoff with jitter), `src/hooks/useErrorRecovery.ts:196` |
| any / as any / as unknown as | none |
| vi.mock | Test files only. No justification comment on most factories. Examples: `src/pages/TestsPage.test.tsx:18`, `src/pages/SummariesPage.test.tsx:14`, `src/App.test.tsx:12`, `src/hooks/useSummariesPageModel.test.tsx:139`, `src/hooks/useTestsPageModel.test.tsx:166`, `src/components/components.smoke.test.tsx:49`, `src/pages/pages.smoke.test.tsx:12`. Commented exceptions: `src/pages/pages.smoke.test.tsx:295`, `src/pages/panels.smoke.test.tsx:22`, `src/pages/TestReportModal.test.tsx:7`. |
| lint / tsc | `npm run lint` (Biome): exit 0, 294 files, no fixes. `npx tsc --noEmit`: exit 0. |
| CogniteClient / DI / ViewModel | `new CogniteClient` / `createCogniteClient`: none. Hook `../` imports: none. Context DI: `src/hooks/useSummariesCdf.ts:59`, `src/hooks/useSummariesPageModel.ts:37`, `src/hooks/useTestsPageModel.ts:36`, `src/hooks/useSummaryGeneration.ts:66`, `src/hooks/useFileContent.ts:22`, `src/hooks/useAgentsCdf.ts:25`, `src/hooks/usePdfBlob.ts:28`, `src/hooks/useSavedPodcast.ts:31`, `src/hooks/usePodcastGeneration.ts:38`, `src/hooks/useComparisonTest.ts:34`, `src/features/dashboard-agent/useAgentLoader.ts:22`, `src/contexts/CdfClientContext.tsx:15`. ViewModel: `src/hooks/useSummariesPageModel.ts:40`, `src/hooks/useTestsPageModel.ts:41`. Pages with SDK/`useQuery`: none under `src/pages/`. Service classes implement interfaces: `src/services/summaryFunctionService.ts:180`, `src/services/podcastFunctionService.ts:141`, `src/services/functionCallClient.ts:57`, `src/services/appConfigService.ts:68`. |
| unused files / console.log | Unused-file scan: `src/vite-env.d.ts` only (allowed coverage exclude, not imported). `console.log` / `console.debug`: none. `console.info`: `src/hooks/useAgentsCdf.ts:125`. `console.error`: error paths in `src/utils/errorHandling/log.ts:13` and several catch blocks. Manual: `src/hooks/useDashboardStats.ts` is imported only by `src/hooks/useDashboardStats.test.tsx`, not by a page. |
| components > 150 lines | Production `.tsx`: `src/components/summaries/SummaryTable.tsx` 259, `src/components/TestingInfoDialog.tsx` 259, `src/pages/SummariesPage.tsx` 254, `src/pages/summaries/SummariesCreateForm.tsx` 245, `src/components/ui/multiselect-dropdown.tsx` 244, `src/pages/tests/TestResultsSections.tsx` 240, `src/components/layout/Navigation.tsx` 238, `src/components/tests/TestTable.tsx` 234, `src/components/common/ConfirmationDialog.tsx` 231, `src/pages/TestConfigCard.tsx` 215, `src/contexts/operations-summarizer/operationsProvider.tsx` 213, `src/components/common/DataTypeControl.tsx` 205, `src/components/common/FilterBar.tsx` 203, `src/pages/SummaryResultPanel.tsx` 202, `src/pages/SummaryDetailModal.tsx` 195, `src/pages/TestReportModal.tsx` 177, `src/pages/TestsPage.tsx` 177, `src/pages/summaries/SummaryDetailHeader.tsx` 175, `src/components/common/ErrorRecoveryDialog.tsx` 158, `src/components/podcast/PodcastTab.tsx` 154, `src/components/summaries/SummaryCard.tsx` 154, `src/components/tests/TestCard.tsx` 151. Pages delegate fetch to ViewModel hooks. |
| flag-bag types | `src/components/podcast/PodcastTab.tsx:34` reads `isLoading` and `isError` from a query result (TanStack state, not a stored flag bag). |
| duplicated DTO types | `src/types/operations.ts:103` `WorkOrderSummary` derives via `Omit`. |
| Route / path | none (`<Route` / `path:`). Navigation is `src/contexts/NavigationContext.tsx` page state, and those pages are rendered. |
| code-quality naming scan | Many `.tsx` basenames are PascalCase (`SummariesPage.tsx`) or kebab-case UI wrappers (`multiselect-dropdown.tsx`). Hook files not prefixed `use`: `src/hooks/cdfQueryKeys.ts`, `src/hooks/summaryGenerationProgress.ts`, `src/hooks/summariesPageModel/creationForm.ts`, `src/hooks/summariesPageModel/tableActions.ts`, `src/hooks/testsPageModel/dateUtils.ts`, `src/hooks/testsPageModel/deriveState.ts`, `src/hooks/testsPageModel/testActions.ts` (helpers, not hooks). |
| commented-out blocks | Consecutive `//` lines are doc comments (for example `src/config/dataTypes.ts:66`, `src/types/operations.ts`), not commented-out code. |

## Must / should / nice

- [ ] Test-report header stats page through every `test_report_*` node until the cursor ends (`limit` up to `instanceQueryLimit`, default ceiling 1000) — `src/utils/testReportStatsStreaming.ts:43` — criterion 2.3 — Should Fix
- [ ] `useDashboardStats` is not mounted by any page — `src/hooks/useDashboardStats.ts:36` — criterion 1.5 — Nice Fix
- [ ] Content-type filtering scans extra DMS pages in the client (capped at 10 extra fetches) because legacy rows have no filterable property — `src/utils/summaryListQuery.ts:128`, `src/utils/summaryListClientFilter.ts:41`, `src/hooks/useSummariesCdf.ts:212` — criterion 2.2 — Nice Fix
- [ ] Test reports still use `instances.list` (full node) while operation summaries use `instances.query` — `src/hooks/useSummariesCdf.ts:180` — criterion 2.1 — Nice Fix
- [ ] Presentational modules over 150 lines (fetch stays in ViewModel hooks) — files listed above — criterion 1.6 — Nice Fix
- [ ] `vi.mock` factories for the host SDK usually have no justification comment — `src/App.test.tsx:12` and the other test hits above — criterion 1.6 — Nice Fix
- [ ] `console.info` when the configured model is unavailable — `src/hooks/useAgentsCdf.ts:125` — criterion 1.5 — Nice Fix
- [ ] Coverage excludes `src/test-utils/**` and `src/__mocks__/**` in addition to the allowed set — `vitest.config.ts:37` — criterion 1.4 — Nice Fix
- [ ] This coverage run exited 1: one Vitest worker timed out starting `src/utils/sanitize.test.ts`, so `src/utils/sanitize.ts` reported 0% lines. 731 other tests passed. — criterion 1.4 — Nice Fix
- [ ] Direct dependencies are within one major of latest; transitive `katex` advisory is low — `package.json` — criterion 1.3 — Nice Fix
- [ ] Component filenames are mostly PascalCase rather than kebab-case — `src/pages/SummariesPage.tsx` — criterion 1.6 — Nice Fix
