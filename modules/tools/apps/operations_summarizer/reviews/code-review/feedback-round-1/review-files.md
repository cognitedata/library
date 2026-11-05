# File inventory: Operations Summarizer

Scope: every `.ts` / `.tsx` under `src/`, plus `vitest.config.ts` and `vite.config.ts`. Excludes `node_modules`, `dist`, and `.cognite-bundles`. Skill copies under `.agents/` and `.claude/` are not app code.

Files: 267.

Structure is the top-level folder. Quality is line count against the 150-line flag (not a defect by itself). Patterns mark ViewModel, context DI, or interface services when those strings are in the file. Non-trivial CDF, page, hook, and service modules were read; barrels and test files were inventoried.

| File | Structure | Quality | Patterns | Tests | Notes |
| ---- | --------- | ------- | -------- | ----- | ----- |
| `src/App.test.tsx` | test | n/a | test | self |  |
| `src/App.tsx` | App.tsx | within 150 | module | yes |  |
| `src/__mocks__/defaultNodeProperties.ts` | __mocks__ | over 150 | module | no | 182 lines; coverage excluded |
| `src/components/Loader.tsx` | components | within 150 | module | no |  |
| `src/components/TestingInfoDialog.test.tsx` | test | n/a | test | self |  |
| `src/components/TestingInfoDialog.tsx` | components | over 150 | module | yes | 259 lines |
| `src/components/common/ConfirmationDialog.tsx` | components | over 150 | module | no | 231 lines |
| `src/components/common/DataTypeControl.test.tsx` | test | n/a | test | self |  |
| `src/components/common/DataTypeControl.tsx` | components | over 150 | module | yes | 205 lines |
| `src/components/common/DownloadErrorDialog.test.tsx` | test | n/a | test | self |  |
| `src/components/common/DownloadErrorDialog.tsx` | components | within 150 | module | yes |  |
| `src/components/common/ErrorRecoveryDialog.test.tsx` | test | n/a | test | self |  |
| `src/components/common/ErrorRecoveryDialog.tsx` | components | over 150 | module | yes | 158 lines |
| `src/components/common/FilterBar.test.tsx` | test | n/a | test | self |  |
| `src/components/common/FilterBar.tsx` | components | over 150 | module | yes | 203 lines |
| `src/components/common/ProgressBar.tsx` | components | within 150 | module | no |  |
| `src/components/common/ResponsiveTable.test.tsx` | test | n/a | test | self |  |
| `src/components/common/ResponsiveTable.tsx` | components | within 150 | module | yes |  |
| `src/components/common/list-table-skeletons.tsx` | components | within 150 | module | no |  |
| `src/components/components.smoke.test.tsx` | test | n/a | test | self |  |
| `src/components/layout/Navigation.test.tsx` | test | n/a | test | self |  |
| `src/components/layout/Navigation.tsx` | components | over 150 | module | yes | 238 lines |
| `src/components/podcast/PodcastTab.test.tsx` | test | n/a | test | self |  |
| `src/components/podcast/PodcastTab.tsx` | components | over 150 | context DI | yes | 154 lines |
| `src/components/summaries/SummaryCard.test.tsx` | test | n/a | test | self |  |
| `src/components/summaries/SummaryCard.tsx` | components | over 150 | module | yes | 154 lines |
| `src/components/summaries/SummaryTable.test.tsx` | test | n/a | test | self |  |
| `src/components/summaries/SummaryTable.tsx` | components | over 150 | module | yes | 259 lines |
| `src/components/tests/TestCard.test.tsx` | test | n/a | test | self |  |
| `src/components/tests/TestCard.tsx` | components | over 150 | module | yes | 151 lines |
| `src/components/tests/TestTable.test.tsx` | test | n/a | test | self |  |
| `src/components/tests/TestTable.tsx` | components | over 150 | module | yes | 234 lines |
| `src/components/ui/alert.tsx` | components | within 150 | module | no |  |
| `src/components/ui/badge.tsx` | components | within 150 | module | no |  |
| `src/components/ui/button.tsx` | components | within 150 | module | no |  |
| `src/components/ui/card.tsx` | components | within 150 | module | no |  |
| `src/components/ui/collapsible-section.tsx` | components | within 150 | module | no |  |
| `src/components/ui/dialog.tsx` | components | within 150 | module | no |  |
| `src/components/ui/input.tsx` | components | within 150 | module | no |  |
| `src/components/ui/label.tsx` | components | within 150 | module | no |  |
| `src/components/ui/multiselect-dropdown.test.tsx` | test | n/a | test | self |  |
| `src/components/ui/multiselect-dropdown.tsx` | components | over 150 | module | yes | 244 lines |
| `src/components/ui/primitives.test.tsx` | test | n/a | test | self |  |
| `src/components/ui/skeleton.tsx` | components | within 150 | module | no |  |
| `src/components/ui/textarea.tsx` | components | within 150 | module | no |  |
| `src/config/appConfig.test.ts` | test | n/a | test | self |  |
| `src/config/appConfig.ts` | config | over 150 | module | yes | 480 lines |
| `src/config/configLocation.ts` | config | within 150 | module | no |  |
| `src/config/dataTypes.test.ts` | test | n/a | test | self |  |
| `src/config/dataTypes.ts` | config | over 150 | module | yes | 226 lines |
| `src/config/viewIds.ts` | config | within 150 | module | no |  |
| `src/contexts/AppConfigContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/AppConfigContext.tsx` | contexts | within 150 | module | yes |  |
| `src/contexts/CdfClientContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/CdfClientContext.tsx` | contexts | within 150 | context DI | yes |  |
| `src/contexts/NavigationContext.tsx` | contexts | within 150 | context DI | no |  |
| `src/contexts/OperationsSummarizerContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/OperationsSummarizerContext.tsx` | contexts | within 150 | module | yes |  |
| `src/contexts/SummariesPageModelDepsContext.tsx` | contexts | within 150 | viewmodel | no |  |
| `src/contexts/TestsPageModelDepsContext.tsx` | contexts | within 150 | viewmodel | no |  |
| `src/contexts/index.ts` | barrel | within 150 | re-export | no |  |
| `src/contexts/operations-summarizer/AgentBootstrap.tsx` | contexts | within 150 | module | no |  |
| `src/contexts/operations-summarizer/agentSessionContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/operations-summarizer/agentSessionContext.tsx` | contexts | within 150 | context DI | yes |  |
| `src/contexts/operations-summarizer/agentSlice.ts` | contexts | within 150 | module | no |  |
| `src/contexts/operations-summarizer/operationsProvider.tsx` | contexts | over 150 | context DI | no | 213 lines |
| `src/contexts/operations-summarizer/rootReducer.ts` | contexts | within 150 | module | no |  |
| `src/contexts/operations-summarizer/summaryGenerationContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/operations-summarizer/summaryGenerationContext.tsx` | contexts | within 150 | context DI | yes |  |
| `src/contexts/operations-summarizer/summaryGenerationSlice.ts` | contexts | within 150 | module | no |  |
| `src/contexts/operations-summarizer/testingSessionContext.test.tsx` | test | n/a | test | self |  |
| `src/contexts/operations-summarizer/testingSessionContext.tsx` | contexts | within 150 | context DI | yes |  |
| `src/contexts/operations-summarizer/testingSessionSlice.ts` | contexts | within 150 | module | no |  |
| `src/contexts/operations-summarizer/types.ts` | contexts | over 150 | module | no | 160 lines |
| `src/features/dashboard-agent/index.hooks.test.tsx` | test | n/a | test | self |  |
| `src/features/dashboard-agent/index.test.ts` | test | n/a | test | self |  |
| `src/features/dashboard-agent/index.ts` | barrel | within 150 | re-export | yes |  |
| `src/features/dashboard-agent/useAgentLoader.test.tsx` | test | n/a | test | self |  |
| `src/features/dashboard-agent/useAgentLoader.ts` | features | within 150 | context DI | yes |  |
| `src/hooks/cdfQueryKeys.ts` | hooks | within 150 | module | no |  |
| `src/hooks/summariesPageModel/creationForm.test.ts` | test | n/a | test | self |  |
| `src/hooks/summariesPageModel/creationForm.ts` | hooks | over 150 | module | yes | 165 lines |
| `src/hooks/summariesPageModel/tableActions.ts` | hooks | within 150 | module | no |  |
| `src/hooks/summaryGenerationProgress.test.ts` | test | n/a | test | self |  |
| `src/hooks/summaryGenerationProgress.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/testsPageModel/dateUtils.ts` | hooks | within 150 | module | no |  |
| `src/hooks/testsPageModel/deriveState.ts` | hooks | within 150 | module | no |  |
| `src/hooks/testsPageModel/testActions.ts` | hooks | within 150 | module | no |  |
| `src/hooks/testsPageModel/useTestsPageFilters.ts` | hooks | within 150 | module | no |  |
| `src/hooks/useAgentsCdf.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useAgentsCdf.ts` | hooks | within 150 | context DI | yes |  |
| `src/hooks/useCdfData.test.ts` | test | n/a | test | self |  |
| `src/hooks/useCdfData.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/useComparisonTest.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useComparisonTest.ts` | hooks | over 150 | context DI | yes | 246 lines |
| `src/hooks/useDashboardStats.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useDashboardStats.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/useDefaultSummariesListPipeline.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useDefaultSummariesListPipeline.ts` | hooks | within 150 | viewmodel | yes |  |
| `src/hooks/useDefaultTestsPageCdfBundle.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useDefaultTestsPageCdfBundle.ts` | hooks | within 150 | viewmodel | yes |  |
| `src/hooks/useEnhancedRefresh.test.ts` | test | n/a | test | self |  |
| `src/hooks/useEnhancedRefresh.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/useErrorRecovery.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useErrorRecovery.ts` | hooks | over 150 | module | yes | 242 lines |
| `src/hooks/useFileContent.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useFileContent.ts` | hooks | within 150 | context DI | yes |  |
| `src/hooks/usePagination.test.ts` | test | n/a | test | self |  |
| `src/hooks/usePagination.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/usePdfBlob.test.tsx` | test | n/a | test | self |  |
| `src/hooks/usePdfBlob.ts` | hooks | within 150 | context DI | yes |  |
| `src/hooks/usePodcastGeneration.test.tsx` | test | n/a | test | self |  |
| `src/hooks/usePodcastGeneration.ts` | hooks | within 150 | viewmodel | yes |  |
| `src/hooks/useRobustDownload.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useRobustDownload.ts` | hooks | over 150 | module | yes | 180 lines |
| `src/hooks/useSavedPodcast.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useSavedPodcast.ts` | hooks | within 150 | viewmodel | yes |  |
| `src/hooks/useSummariesCdf.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useSummariesCdf.ts` | hooks | over 150 | context DI | yes | 503 lines |
| `src/hooks/useSummariesPageModel.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useSummariesPageModel.ts` | hooks | over 150 | viewmodel | yes | 264 lines |
| `src/hooks/useSummaryFilters.test.ts` | test | n/a | test | self |  |
| `src/hooks/useSummaryFilters.ts` | hooks | over 150 | module | yes | 292 lines |
| `src/hooks/useSummaryGeneration.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useSummaryGeneration.ts` | hooks | over 150 | viewmodel | yes | 305 lines |
| `src/hooks/useTestingState.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useTestingState.ts` | hooks | within 150 | module | yes |  |
| `src/hooks/useTestsPageModel.test.tsx` | test | n/a | test | self |  |
| `src/hooks/useTestsPageModel.ts` | hooks | over 150 | viewmodel | yes | 305 lines |
| `src/lib/pageColumnWidth.ts` | lib | within 150 | module | no |  |
| `src/lib/utils.ts` | lib | within 150 | module | no |  |
| `src/main.tsx` | main.tsx | within 150 | module | no | coverage excluded (allowed) |
| `src/pages/SettingsPage.test.tsx` | test | n/a | test | self |  |
| `src/pages/SettingsPage.tsx` | pages | within 150 | module | yes |  |
| `src/pages/SummariesPage.test.tsx` | test | n/a | test | self |  |
| `src/pages/SummariesPage.tsx` | pages | over 150 | module | yes | 254 lines |
| `src/pages/SummaryDetailModal.test.tsx` | test | n/a | test | self |  |
| `src/pages/SummaryDetailModal.tsx` | pages | over 150 | module | yes | 195 lines |
| `src/pages/SummaryResultPanel.test.tsx` | test | n/a | test | self |  |
| `src/pages/SummaryResultPanel.tsx` | pages | over 150 | module | yes | 202 lines |
| `src/pages/TestConfigCard.test.tsx` | test | n/a | test | self |  |
| `src/pages/TestConfigCard.tsx` | pages | over 150 | module | yes | 215 lines |
| `src/pages/TestReportModal.test.tsx` | test | n/a | test | self |  |
| `src/pages/TestReportModal.tsx` | pages | over 150 | module | yes | 177 lines |
| `src/pages/TestResultsPanel.test.tsx` | test | n/a | test | self |  |
| `src/pages/TestResultsPanel.tsx` | pages | within 150 | module | yes |  |
| `src/pages/TestsPage.test.tsx` | test | n/a | test | self |  |
| `src/pages/TestsPage.tsx` | pages | over 150 | module | yes | 177 lines |
| `src/pages/pages.smoke.test.tsx` | test | n/a | test | self |  |
| `src/pages/panels.smoke.test.tsx` | test | n/a | test | self |  |
| `src/pages/settings/AgentRequiredCard.tsx` | pages | within 150 | module | no |  |
| `src/pages/settings/AgentStatusCard.tsx` | pages | within 150 | module | no |  |
| `src/pages/settings/PromptsCard.tsx` | pages | within 150 | module | no |  |
| `src/pages/settings/SettingsHeader.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/CreateSummaryDateFields.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/CreateSummaryFeedback.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/CreateSummaryFieldGroups.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/CreateSummaryHeader.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/SummariesCreateForm.test.tsx` | test | n/a | test | self |  |
| `src/pages/summaries/SummariesCreateForm.tsx` | pages | over 150 | module | yes | 245 lines |
| `src/pages/summaries/SummaryDetailBody.tsx` | pages | within 150 | module | no |  |
| `src/pages/summaries/SummaryDetailHeader.tsx` | pages | over 150 | module | no | 175 lines |
| `src/pages/summaries/SummaryPdfViewer.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestResultsSections.tsx` | pages | over 150 | module | no | 240 lines |
| `src/pages/tests/TestsCreateBaselineFields.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsCreateForm.test.tsx` | test | n/a | test | self |  |
| `src/pages/tests/TestsCreateForm.tsx` | pages | within 150 | module | yes |  |
| `src/pages/tests/TestsCreateFormHeader.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsCreateFormPrompts.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsFetchErrorBanner.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsHistoryBar.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsHistoryScoreRange.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsHistoryStats.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsPageToolbar.test.tsx` | test | n/a | test | self |  |
| `src/pages/tests/TestsPageToolbar.tsx` | pages | within 150 | module | yes |  |
| `src/pages/tests/TestsReportsSection.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsScoreLegend.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsToolbarActions.tsx` | pages | within 150 | module | no |  |
| `src/pages/tests/TestsToolbarTitle.tsx` | pages | within 150 | module | no |  |
| `src/services/appConfigService.test.ts` | test | n/a | test | self |  |
| `src/services/appConfigService.ts` | services | over 150 | interface service | yes | 172 lines |
| `src/services/cdfHelper.test.ts` | test | n/a | test | self |  |
| `src/services/cdfHelper.ts` | services | over 150 | module | yes | 330 lines |
| `src/services/functionCallClient.test.ts` | test | n/a | test | self |  |
| `src/services/functionCallClient.ts` | services | over 150 | module | yes | 236 lines |
| `src/services/podcastFiles.test.ts` | test | n/a | test | self |  |
| `src/services/podcastFiles.ts` | services | within 150 | module | yes |  |
| `src/services/podcastFunctionService.test.ts` | test | n/a | test | self |  |
| `src/services/podcastFunctionService.ts` | services | over 150 | interface service | yes | 167 lines |
| `src/services/prompts.ts` | services | within 150 | module | no |  |
| `src/services/summaryFunctionService.test.ts` | test | n/a | test | self |  |
| `src/services/summaryFunctionService.ts` | services | over 150 | interface service | yes | 203 lines |
| `src/test-utils/mockSdk.ts` | test-utils | within 150 | module | no | coverage excluded |
| `src/test-utils/testDoubles.ts` | test-utils | within 150 | module | no | coverage excluded |
| `src/test-utils/wrappers.tsx` | test-utils | within 150 | module | no | coverage excluded |
| `src/types/appConfig.ts` | types | within 150 | module | no |  |
| `src/types/operations.ts` | types | over 150 | module | no | 400 lines |
| `src/utils/api.ts` | utils | within 150 | module | no |  |
| `src/utils/array.test.ts` | test | n/a | test | self |  |
| `src/utils/array.ts` | utils | within 150 | module | yes |  |
| `src/utils/cdfRequest.test.ts` | test | n/a | test | self |  |
| `src/utils/cdfRequest.ts` | utils | within 150 | module | yes |  |
| `src/utils/cdfRetry.test.ts` | test | n/a | test | self |  |
| `src/utils/cdfRetry.ts` | utils | within 150 | module | yes |  |
| `src/utils/dates.test.ts` | test | n/a | test | self |  |
| `src/utils/dates.ts` | utils | over 150 | module | yes | 164 lines |
| `src/utils/errorHandling.test.ts` | test | n/a | test | self |  |
| `src/utils/errorHandling.ts` | utils | within 150 | module | yes |  |
| `src/utils/errorHandling/actionMessages.ts` | utils | within 150 | module | no |  |
| `src/utils/errorHandling/categorize.ts` | utils | within 150 | module | no |  |
| `src/utils/errorHandling/index.ts` | barrel | within 150 | re-export | no |  |
| `src/utils/errorHandling/log.ts` | utils | within 150 | module | no |  |
| `src/utils/errorHandling/parse.ts` | utils | within 150 | module | no |  |
| `src/utils/errorHandling/types.ts` | utils | within 150 | module | no |  |
| `src/utils/filenames.test.ts` | test | n/a | test | self |  |
| `src/utils/filenames.ts` | utils | within 150 | module | yes |  |
| `src/utils/formatting.test.ts` | test | n/a | test | self |  |
| `src/utils/formatting.ts` | utils | within 150 | module | yes |  |
| `src/utils/fusionFileUrl.test.ts` | test | n/a | test | self |  |
| `src/utils/fusionFileUrl.ts` | utils | within 150 | module | yes |  |
| `src/utils/markdown-format.test.ts` | test | n/a | test | self |  |
| `src/utils/markdown-format.ts` | utils | within 150 | module | yes |  |
| `src/utils/nodeProperties.test.ts` | test | n/a | test | self |  |
| `src/utils/nodeProperties.ts` | utils | within 150 | module | yes |  |
| `src/utils/pagination.test.ts` | test | n/a | test | self |  |
| `src/utils/pagination.ts` | utils | within 150 | module | yes |  |
| `src/utils/pdfGenerator.test.ts` | test | n/a | test | self |  |
| `src/utils/pdfGenerator.ts` | utils | within 150 | module | yes |  |
| `src/utils/pdfGenerator/index.ts` | barrel | within 150 | re-export | no |  |
| `src/utils/pdfGenerator/markdownRenderer.ts` | utils | within 150 | module | no |  |
| `src/utils/pdfGenerator/textRenderer.ts` | utils | over 150 | module | no | 180 lines |
| `src/utils/pdfGenerator/types.ts` | utils | within 150 | module | no |  |
| `src/utils/sanitize.test.ts` | test | n/a | test | self |  |
| `src/utils/sanitize.ts` | utils | within 150 | module | yes |  |
| `src/utils/scores.test.ts` | test | n/a | test | self |  |
| `src/utils/scores.ts` | utils | over 150 | module | yes | 270 lines |
| `src/utils/semaphore.test.ts` | test | n/a | test | self |  |
| `src/utils/semaphore.ts` | utils | within 150 | module | yes |  |
| `src/utils/summaryContentTypes.test.ts` | test | n/a | test | self |  |
| `src/utils/summaryContentTypes.ts` | utils | within 150 | module | yes |  |
| `src/utils/summaryDownload.test.ts` | test | n/a | test | self |  |
| `src/utils/summaryDownload.ts` | utils | over 150 | module | yes | 304 lines |
| `src/utils/summaryGenerationTypes.ts` | utils | within 150 | module | no |  |
| `src/utils/summaryInstanceMapping.test.ts` | test | n/a | test | self |  |
| `src/utils/summaryInstanceMapping.ts` | utils | within 150 | module | yes |  |
| `src/utils/summaryListClientFilter.test.ts` | test | n/a | test | self |  |
| `src/utils/summaryListClientFilter.ts` | utils | within 150 | module | yes |  |
| `src/utils/summaryListQuery.test.ts` | test | n/a | test | self |  |
| `src/utils/summaryListQuery.ts` | utils | over 150 | module | yes | 315 lines |
| `src/utils/testReportStatsStreaming.test.ts` | test | n/a | test | self |  |
| `src/utils/testReportStatsStreaming.ts` | utils | within 150 | module | yes |  |
| `src/utils/testReportUtils.test.ts` | test | n/a | test | self |  |
| `src/utils/testReportUtils.ts` | utils | within 150 | module | yes |  |
| `src/utils/testReportUtils/contentExtraction.ts` | utils | within 150 | module | no |  |
| `src/utils/testReportUtils/index.ts` | barrel | within 150 | re-export | no |  |
| `src/utils/testReportUtils/scoreDisplay.ts` | utils | within 150 | module | no |  |
| `src/utils/testReportUtils/scoreExtraction.ts` | utils | within 150 | module | no |  |
| `src/utils/testReportUtils/stats.ts` | utils | within 150 | module | no |  |
| `src/utils/testingContent.test.ts` | test | n/a | test | self |  |
| `src/utils/testingContent.ts` | utils | over 150 | module | yes | 188 lines |
| `src/utils/testingSave.test.ts` | test | n/a | test | self |  |
| `src/utils/testingSave.ts` | utils | over 150 | module | yes | 197 lines |
| `src/utils/testingYaml.test.ts` | test | n/a | test | self |  |
| `src/utils/testingYaml.ts` | utils | within 150 | module | yes |  |
| `src/vite-env.d.ts` | vite-env.d.ts | within 150 | module | no | coverage excluded (allowed) |
| `vite.config.ts` | config | within 150 | module | no |  |
| `vitest.config.ts` | config | within 150 | module | no |  |
