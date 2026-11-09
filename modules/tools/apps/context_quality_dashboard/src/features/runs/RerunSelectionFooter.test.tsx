import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { LanguageProvider } from '../../i18n/LanguageProvider';
import { translate } from '../../i18n/messages';
import type { RunBannerViewModel } from '../shell/useRunBannerViewModel';

import { METRICS_MODEL_FILTER_ALL } from './rerunSelectionContext';
import { RerunSelectionFooter } from './RerunSelectionFooter';
import type { CheckState } from './selectionTree';
import type { RerunViewModel } from './useRerunViewModel';

describe(RerunSelectionFooter.name, () => {
  it('should show a disabled run button and say nothing is selected when nothing is selected', () => {
    render(<RerunSelectionFooter viewModel={viewModel({ selectedTotal: 0 })} />);

    expect(screen.getByRole('button', { name: translate('rerun.summary.runNone', 'en') })).toBeDisabled();
    expect(screen.getByTestId('rerun-summary-total')).toHaveTextContent(translate('rerun.summary.none', 'en'));
  });

  it('should show the outcome of the run below the button, with its action and dismiss', async () => {
    const user = userEvent.setup();
    const dismiss = vi.fn();
    const onPrimaryAction = vi.fn();
    render(
      <RerunSelectionFooter
        viewModel={viewModel({
          runBanner: banner('success', { title: 'Run completed successfully', body: 'The latest results are now available.', dismiss, onPrimaryAction }),
        })}
      />
    );

    expect(screen.getByTestId('rerun-run-status')).toHaveTextContent(
      'Run completed successfully — The latest results are now available.'
    );
    await user.click(screen.getByRole('button', { name: 'View results' }));
    await user.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(onPrimaryAction).toHaveBeenCalledTimes(1);
    expect(dismiss).toHaveBeenCalledTimes(1);
  });

  it('should show a run that did not complete below the button', () => {
    render(
      <RerunSelectionFooter
        viewModel={viewModel({ runBanner: banner('failed', { title: 'Run did not complete', body: 'Check the run details.' }) })}
      />
    );

    expect(screen.getByTestId('rerun-run-status')).toHaveTextContent('Run did not complete — Check the run details.');
  });

  it('should show no run status when there is no run to report', () => {
    render(<RerunSelectionFooter viewModel={viewModel({})} />);

    expect(screen.queryByTestId('rerun-run-status')).not.toBeInTheDocument();
  });

  it('should show counts and open confirm on run', async () => {
    const user = userEvent.setup();
    render(
      <LanguageProvider>
        <RerunSelectionFooter
          viewModel={viewModel({
            selectedTotal: 2,
            selectedDimensions: ['files'],
            selectedMetrics: [{ id: 'm1', view: 'V', property: 'p', model: 'M', link: 'V.p' }],
            canRun: true,
            request: {
              kind: 'selection',
              dimensionIds: ['files'],
              links: [{ model: 'M', link: 'V.p' }],
            },
          })}
        />
      </LanguageProvider>
    );

    expect(screen.getByTestId('rerun-summary-total')).toHaveTextContent('2 items selected');
    await user.click(screen.getByTestId('rerun-run-selected'));
    expect(await screen.findByTestId('confirm-run-dialog')).toBeInTheDocument();
  });

  it('should show a running state on the button while a run is in progress', () => {
    render(
      <LanguageProvider>
        <RerunSelectionFooter
          viewModel={viewModel({
            selectedTotal: 1,
            selectedDimensions: ['files'],
            canRun: false,
            busy: true,
            request: { kind: 'selection', dimensionIds: ['files'], links: [] },
          })}
        />
      </LanguageProvider>
    );

    expect(screen.getByTestId('rerun-run-selected')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Running' })).toBeInTheDocument();
    expect(screen.getByTestId('rerun-summary-busy')).toHaveTextContent(
      'A run is in progress. You can start another when it finishes.'
    );
  });
});

function banner(
  variant: 'running' | 'success' | 'failed',
  input: { title: string; body: string; dismiss?: () => void; onPrimaryAction?: () => void }
): RunBannerViewModel {
  return {
    banner: {
      visible: true,
      variant,
      source: 'config',
      dismissible: input.dismiss !== undefined,
      copy: { titleKey: '', bodyKey: '' },
    },
    title: input.title,
    body: input.body,
    line: `${input.title} — ${input.body}`,
    dismiss: input.dismiss ?? null,
    primaryActionLabel: input.onPrimaryAction === undefined ? '' : 'View results',
    dismissLabel: 'Dismiss',
    onPrimaryAction: input.onPrimaryAction ?? null,
  };
}

const NO_RUN_BANNER: RunBannerViewModel = {
  banner: { visible: false, variant: 'running', source: 'config', dismissible: false, copy: { titleKey: '', bodyKey: '' } },
  title: '',
  body: '',
  line: '',
  dismiss: null,
  primaryActionLabel: '',
  dismissLabel: 'Dismiss',
  onPrimaryAction: null,
};

function viewModel(overrides: Partial<RerunViewModel>): RerunViewModel {
  const t = (key: string) => translate(key, 'en');
  return {
    t,
    tab: 'dimensions',
    setTab: vi.fn(),
    dimensionIds: ['asset-hierarchy', 'files'],
    selectedDimensions: [],
    selectedMetrics: [],
    selectedTotal: 0,
    totalMetrics: 3,
    tree: [],
    metricsTree: [],
    dataModelIds: [],
    metricsModelFilter: METRICS_MODEL_FILTER_ALL,
    setMetricsModelFilter: vi.fn(),
    isDimensionSelected: vi.fn(() => false),
    isDimensionNotMeasured: vi.fn(() => false),
    isMetricSelected: vi.fn(() => false),
    toggleDimension: vi.fn(),
    toggleMetric: vi.fn(),
    toggleGroup: vi.fn(),
    toggleAllDimensions: vi.fn(),
    toggleAllMetrics: vi.fn(),
    groupState: vi.fn((): CheckState => 'unchecked'),
    allDimensionsState: 'unchecked',
    allMetricsState: 'unchecked',
    phase: 'idle',
    startError: null,
    lastRequested: null,
    canRun: false,
    busy: false,
    otherRunInProgress: false,
    runAvailable: true,
    request: null,
    start: vi.fn(),
    runBanner: NO_RUN_BANNER,
    ...overrides,
  };
}
