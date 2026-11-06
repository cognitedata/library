import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { Progress } from '@cognite/aura/components/progress';
import {
  IconCircleCheckFilled,
  IconInfoCircleFilled,
  IconSettings,
  IconX,
} from '@cognite/aura/icons';
import { useState } from 'react';

import { BuiltInChecksCoverageSidePanel } from './BuiltInChecksCoverageSidePanel';
import type { BuiltInOnboardingStatus } from './builtInOnboardingStatus';
import { RunConfigEditorCard } from './RunConfigEditorCard';
import { RunConfigExampleDownload } from './RunConfigExampleDownload';
import { useFirstRunOnboardingViewModel } from './useFirstRunOnboardingViewModel';

function fill(template: string, values: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => values[name] ?? '');
}

const onboardingShellClassName = 'mx-auto w-full max-w-[880px] space-y-6 py-1';

export function FirstRunOnboardingPage() {
  const viewModel = useFirstRunOnboardingViewModel();
  const { t, fileViewModel, optionalChecksSkipped } = viewModel;
  const hasConfigDraft = fileViewModel.fileName !== null || fileViewModel.text.trim() !== '';
  const showExampleHelpBanner = !hasConfigDraft && !optionalChecksSkipped;

  return (
    <div className={onboardingShellClassName} data-testid="first-run-onboarding">
      <header>
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {t('configuration.firstRunOnboarding.welcome.eyebrow')}
        </p>
        <h1 className="mt-2 text-2xl font-semibold tracking-tight text-foreground">
          {t('configuration.firstRunOnboarding.welcome.title')}
        </h1>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted-foreground">
          {t(viewModel.welcomeSubtitleKey)}
        </p>
      </header>

      <div className="space-y-2">
        <BuiltInStatusCard viewModel={viewModel} />
        <BuiltInCoverageFootnote viewModel={viewModel} />
      </div>

      <ConfiguredOptionalSection viewModel={viewModel} showExampleHelpBanner={showExampleHelpBanner} />
    </div>
  );
}

type ConfiguredOptionalSectionProps = {
  viewModel: ReturnType<typeof useFirstRunOnboardingViewModel>;
  showExampleHelpBanner: boolean;
};

function ConfiguredOptionalSection({ viewModel, showExampleHelpBanner }: ConfiguredOptionalSectionProps) {
  const { t, fileViewModel, optionalChecksSkipped } = viewModel;

  if (optionalChecksSkipped) {
    return (
      <div className="space-y-2" data-testid="first-run-configured-skipped">
        <Card
          className="border border-border bg-card-background shadow-none"
          data-testid="first-run-step-configured"
        >
          <CardContent className="gap-0 p-4">
            <div className="flex w-full items-start gap-3">
              <span
                className="flex size-10 shrink-0 items-center justify-center rounded-lg border border-border bg-muted/30"
                aria-hidden="true"
              >
                <IconSettings className="size-5 text-muted-foreground" />
              </span>
              <div className="flex min-w-0 flex-1 items-start justify-between gap-4">
                <div className="min-w-0 flex-1 space-y-1">
                  <h2 className="text-sm font-semibold text-foreground">
                    {t('configuration.firstRunOnboarding.configured.cardTitle')}
                  </h2>
                  <p className="text-sm text-muted-foreground">
                    {t('configuration.firstRunOnboarding.configured.body')}
                  </p>
                </div>
                <Badge
                  variant="secondary"
                  className="w-fit shrink-0 font-normal"
                  data-testid="first-run-configured-skipped-badge"
                >
                  {t('configuration.firstRunOnboarding.configured.statusLabel.skipped')}
                </Badge>
              </div>
            </div>
          </CardContent>
        </Card>
        <p className="text-sm text-muted-foreground" data-testid="first-run-configured-skipped-footnote">
          {t('configuration.firstRunOnboarding.configured.skippedFootnote')}
        </p>
        <div className="flex justify-end pt-1">
          <Button
            type="button"
            data-testid="first-run-continue-after-skip"
            onClick={viewModel.goToOverview}
          >
            {t('configuration.firstRunOnboarding.configured.continueButton')}
          </Button>
        </div>
      </div>
    );
  }

  return (
    <Card
      className="border border-border bg-card-background shadow-none"
      data-testid="first-run-step-configured"
    >
      <CardContent className="space-y-5 p-5">
        <div className="flex gap-3">
          <span
            className="flex size-10 shrink-0 items-center justify-center rounded-lg border border-border bg-muted/30"
            aria-hidden="true"
          >
            <IconSettings className="size-5 text-muted-foreground" />
          </span>
          <div className="min-w-0 flex-1 space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-base font-semibold text-foreground">
                {t('configuration.firstRunOnboarding.configured.cardTitle')}
              </h2>
              <Badge variant="secondary" className="font-normal">
                {t('configuration.firstRunOnboarding.configured.optional')}
              </Badge>
            </div>
            <p className="text-sm text-muted-foreground">
              {t('configuration.firstRunOnboarding.configured.body')}
            </p>
          </div>
        </div>

        {showExampleHelpBanner ? <RunConfigExampleDownload t={t} /> : null}

        <RunConfigEditorCard
          viewModel={fileViewModel}
          hidePrimaryRunButton
          hideInlineRunStatus
          embedded
          compactUpload
        />

        {fileViewModel.result !== null && fileViewModel.result.ok === false ? (
          <p className="text-sm text-destructive-foreground" role="alert" data-testid="first-run-config-validation">
            {t('configuration.firstRunOnboarding.validation.invalid')}
          </p>
        ) : null}

        <div className="flex w-full flex-row flex-wrap justify-end gap-3 border-t border-border pt-4">
          <Button type="button" variant="outline" data-testid="first-run-skip" onClick={viewModel.skip}>
            {t('configuration.firstRunOnboarding.skip.button')}
          </Button>
          <Button
            type="button"
            data-testid="first-run-run-additional"
            disabled={!viewModel.runAdditionalEnabled}
            onClick={viewModel.runAdditionalChecks}
          >
            {viewModel.runAdditionalInProgress
              ? t('configuration.firstRunOnboarding.runAdditional.loading')
              : t('configuration.firstRunOnboarding.runAdditional.button')}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

type BuiltInStatusCardProps = {
  viewModel: ReturnType<typeof useFirstRunOnboardingViewModel>;
};

function BuiltInStatusCard({ viewModel }: BuiltInStatusCardProps) {
  const { t, builtInStatus, builtInFailureDetail } = viewModel;
  const statusLabel = t(viewModel.builtInStatusLabelKey);
  const showRunningProgress = builtInStatus === 'running' || builtInStatus === 'pending';

  return (
    <Card
      className="border border-border bg-card-background shadow-none"
      data-testid="first-run-step-built-in"
    >
      <CardContent className="gap-0 p-4">
        <div className="flex w-full items-start gap-3">
          <BuiltInIcon status={builtInStatus} />
          <div className="flex min-w-0 flex-1 items-start justify-between gap-4">
            <div className="min-w-0 flex-1 space-y-1">
              <h2 className="text-sm font-semibold text-foreground">
                {t('configuration.firstRunOnboarding.builtIn.cardTitle')}
              </h2>
              <p className="text-sm text-muted-foreground">
                {t('configuration.firstRunOnboarding.builtIn.cardBody')}
              </p>
              {builtInStatus === 'unavailable' ? (
                <p className="text-sm text-muted-foreground" data-testid="first-run-built-in-status">
                  {t('configuration.firstRun.noRun')}
                </p>
              ) : null}
              {builtInFailureDetail !== null ? (
                <p className="text-sm text-destructive-foreground" data-testid="first-run-built-in-status">
                  {fill(t(builtInFailureDetail.key), { message: builtInFailureDetail.message })}{' '}
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-auto p-0 text-sm text-primary underline"
                    data-testid="first-run-built-in-error-details"
                    onClick={viewModel.openRunDetails}
                  >
                    {t('configuration.firstRunOnboarding.builtIn.viewDetails')}
                  </Button>
                </p>
              ) : null}
            </div>
            <BuiltInStatusIndicator
              status={builtInStatus}
              label={statusLabel}
              showProgress={showRunningProgress}
              progressLabel={t('configuration.run.progressLabel')}
            />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

type BuiltInCoverageFootnoteProps = {
  viewModel: ReturnType<typeof useFirstRunOnboardingViewModel>;
};

function BuiltInCoverageFootnote({ viewModel }: BuiltInCoverageFootnoteProps) {
  const { t } = viewModel;
  const [coveragePanelOpen, setCoveragePanelOpen] = useState(false);

  return (
    <>
      <p className="text-sm text-muted-foreground" data-testid="first-run-built-in-coverage">
        {t('configuration.firstRunOnboarding.builtIn.coverageQuestion')}{' '}
        <Button
          type="button"
          variant="ghost"
          aria-label={t('configuration.firstRunOnboarding.builtIn.coverageView')}
          className="inline h-auto p-0 text-sm font-normal text-primary underline underline-offset-2 hover:text-primary/90"
          data-testid="first-run-built-in-coverage-view"
          onClick={() => setCoveragePanelOpen(true)}
        >
          {t('configuration.firstRunOnboarding.builtIn.coverageView')}
        </Button>
      </p>
      <BuiltInChecksCoverageSidePanel
        open={coveragePanelOpen}
        onClose={() => setCoveragePanelOpen(false)}
        t={t}
      />
    </>
  );
}

function BuiltInIcon({ status }: { status: BuiltInOnboardingStatus }) {
  const iconClassName = 'size-5';
  const wrapClassName =
    'flex size-10 shrink-0 items-center justify-center rounded-lg border border-border bg-muted/30';

  if (status === 'completed') {
    return (
      <span className={wrapClassName} aria-hidden="true">
        <IconCircleCheckFilled className={`${iconClassName} text-success-foreground`} />
      </span>
    );
  }
  if (status === 'failed') {
    return (
      <span className={wrapClassName} aria-hidden="true">
        <IconX className={`${iconClassName} text-destructive-foreground`} />
      </span>
    );
  }
  return (
    <span className={wrapClassName} aria-hidden="true">
      <IconInfoCircleFilled className={`${iconClassName} text-info-foreground`} />
    </span>
  );
}

function BuiltInStatusIndicator({
  status,
  label,
  showProgress,
  progressLabel,
}: {
  status: BuiltInOnboardingStatus;
  label: string;
  showProgress: boolean;
  progressLabel: string;
}) {
  const variant =
    status === 'completed'
      ? 'success'
      : status === 'failed'
        ? 'error'
        : status === 'running' || status === 'pending'
          ? 'inProgress'
          : 'secondary';

  return (
    <div
      className="flex shrink-0 flex-col items-end self-start"
      data-testid="first-run-built-in-status-rail"
    >
      <Badge variant={variant} className="w-fit shrink-0 font-normal" data-testid="first-run-built-in-badge">
        {label}
      </Badge>
      {showProgress ? (
        <Progress
          value={null}
          size="sm"
          className="mt-2 w-full min-w-[8.5rem] max-w-[8.5rem]"
          aria-label={progressLabel}
          data-testid="first-run-built-in-progress"
        />
      ) : null}
    </div>
  );
}
