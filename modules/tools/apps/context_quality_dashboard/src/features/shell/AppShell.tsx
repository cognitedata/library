import { useEffect, useRef } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { isBackendError } from '../../services/cdf/backendErrors';
import { useRunService } from '../../services/useServices';
import { AlertsPage } from '../alerts/AlertsPage';
import { BackendErrorPanel } from '../backend/BackendErrorPanel';
import { DemoModeBanner } from '../backend/DemoModeBanner';
import { shouldShowConfigurationOnboarding } from '../configuration/configurationOnboarding';
import { ConfigurationPage } from '../configuration/ConfigurationPage';
import { FirstRunOnboardingPage } from '../configuration/FirstRunOnboardingPage';
import { DimensionFilterBarLeading } from '../dimension/DimensionFilterBarLeading';
import { DimensionPage } from '../dimension/DimensionPage';
import { IssuesPage } from '../issues/IssuesPage';
import { OverviewPage } from '../overview/OverviewPage';
import { RunNotes } from '../runNotes/RunNotes';
import { RunLauncherProvider } from '../runs/RunLauncherProvider';

import { appContentHorizontalInsetClassName } from './appContentHorizontalInset';
import { AppTopbar } from './AppTopbar';
import { DownloadUnlinkedButton } from './DownloadUnlinkedButton';
import { FilterBar } from './FilterBar';
import { QualityOperationsProvider } from './QualityOperationsProvider';
import { RunExecutionProvider } from './RunExecutionProvider';
import { RunStatusBanner } from './RunStatusBanner';
import { RunStatusIndicator } from './RunStatusIndicator';
import { useAppShellViewModel } from './useAppShellViewModel';
import { useQualityOperations } from './useQualityOperations';

export function AppShell() {
  const viewModel = useAppShellViewModel();

  return (
    <QualityOperationsProvider>
      <RunLauncherProvider>
        <RunExecutionProvider>
          <AppShellContent viewModel={viewModel} />
        </RunExecutionProvider>
      </RunLauncherProvider>
    </QualityOperationsProvider>
  );
}

type AppShellContentProps = {
  viewModel: ReturnType<typeof useAppShellViewModel>;
};

function AppShellContent({ viewModel }: AppShellContentProps) {
  const { state, dispatch } = useAppState();
  const operations = useQualityOperations();
  const canStartRun = useRunService() !== null;
  const backendError = operations.runStatusError;
  const hasNoRunYet = isBackendError(backendError) && backendError.kind === 'no-completed-run';
  const configurationOnboarding = shouldShowConfigurationOnboarding(state, hasNoRunYet);
  const hideNoRunErrorOnConfiguration =
    hasNoRunYet &&
    viewModel.page === 'configuration' &&
    (configurationOnboarding || state.configurationOnboardingComplete === true);
  const showError = Boolean(backendError) && !hideNoRunErrorOnConfiguration;

  const onboardingInitialRedirectDone = useRef(false);
  useEffect(() => {
    if (!configurationOnboarding || onboardingInitialRedirectDone.current) {
      return;
    }
    onboardingInitialRedirectDone.current = true;
    if (viewModel.page !== 'configuration') {
      viewModel.setPage('configuration');
    }
  }, [configurationOnboarding, viewModel]);

  useEffect(() => {
    if (state.configurationOnboardingComplete === true) {
      return;
    }
    if (operations.runStatus === undefined && operations.runStatusError == null) {
      return;
    }
    if (hasNoRunYet) {
      return;
    }
    dispatch({ type: 'setConfigurationOnboardingComplete', complete: true });
  }, [
    dispatch,
    hasNoRunYet,
    operations.runStatus,
    operations.runStatusError,
    state.configurationOnboardingComplete,
  ]);

  useEffect(() => {
    if (state.configurationOnboardingComplete === true || state.configurationOptionalSkipped !== true) {
      return;
    }
    if (viewModel.page === 'configuration') {
      return;
    }
    dispatch({ type: 'setConfigurationOnboardingComplete', complete: true });
  }, [dispatch, state.configurationOnboardingComplete, state.configurationOptionalSkipped, viewModel.page]);

  // The document scrolls, so a new page would otherwise open at the scroll offset of the page before it.
  useEffect(() => {
    window.scrollTo({ top: 0, left: 0 });
  }, [viewModel.page]);

  useEffect(() => {
    if (viewModel.page !== 'dimensions' || state.runNotesOpen !== true) {
      return;
    }
    dispatch({ type: 'setRunNotesOpen', open: false });
  }, [dispatch, state.runNotesOpen, viewModel.page]);

  return (
    <div className="flex min-h-screen flex-col bg-background text-foreground">
      <DemoModeBanner />
      <div className="sticky top-0 z-20 flex flex-col bg-background">
        <RunStatusBanner />
        <AppTopbar
          page={viewModel.page}
          language={viewModel.language}
          themeMode={viewModel.themeMode}
          showPageTabs={!configurationOnboarding}
          onPageChange={viewModel.setPage}
          onLanguageChange={viewModel.setLanguage}
          onThemeChange={viewModel.setTheme}
          t={viewModel.t}
        />

        {viewModel.showFilters && !backendError ? (
          <FilterBar
            filters={viewModel.filters}
            spaces={viewModel.spaces}
            locations={viewModel.locations}
            trendWindow={viewModel.trendWindow}
            isLoading={viewModel.isCatalogLoading}
            onFilterChange={viewModel.setFilter}
            onTrendWindowChange={viewModel.setTrendWindow}
            leading={
              viewModel.page === 'dimensions' ? <DimensionFilterBarLeading /> : undefined
            }
            trailing={
              <>
                <RunStatusIndicator />
                <DownloadUnlinkedButton />
                {viewModel.page === 'dimensions' ? null : <RunNotes />}
              </>
            }
            showFilterFields={viewModel.showFilterFields}
            t={viewModel.t}
          />
        ) : null}
      </div>

      <main className={`flex-1 py-4 ${appContentHorizontalInsetClassName}`}>
        {showError ? (
          <BackendErrorPanel
            error={backendError}
            onRetry={operations.retryRunStatus}
            onGoToConfiguration={() => viewModel.setPage('configuration')}
            canStartRun={canStartRun}
          />
        ) : viewModel.page === 'overview' ? (
          <OverviewPage />
        ) : viewModel.page === 'dimensions' ? (
          <DimensionPage />
        ) : viewModel.page === 'issues' ? (
          <IssuesPage />
        ) : viewModel.page === 'alerts' ? (
          <AlertsPage />
        ) : viewModel.page === 'configuration' ? (
          configurationOnboarding ? (
            <FirstRunOnboardingPage />
          ) : (
            <ConfigurationPage />
          )
        ) : (
          <p className="text-base" data-testid="page-content">{viewModel.pageLabel}</p>
        )}
      </main>
    </div>
  );
}
