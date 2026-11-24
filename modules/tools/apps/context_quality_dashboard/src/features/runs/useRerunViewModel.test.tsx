import { renderHook, act } from '@testing-library/react';
import type { ComponentType, ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { RerunSelectionProvider } from './RerunSelectionProvider';
import { useRerunViewModel } from './useRerunViewModel';

// Mocked: the stored config file is read from CDF Files; these tests set what it holds.
vi.mock('../configuration/useRunConfigFileViewModel', () => ({
  useRunConfigFileViewModel: () => ({
    t: (key: string) => key,
    models: [
      { model: 'a:B:v1', links: ['Asset.name', 'Asset.parent'] },
      { model: 'a:B:v1', links: ['Pump.tag'] },
      { model: 'c:D:v2', links: ['Tank.level'] },
    ],
    phase: 'idle',
    startError: null,
  }),
}));

let dimensionRuns: Record<string, string> | undefined = { files: '2026-11-04T06:00:00.000Z' };
// Mocked: run status comes from polling the workflow; the tests set the last run times directly.
vi.mock('../shell/useQualityOperations', () => ({
  useQualityOperations: () => ({ runStatus: { dimensionRuns } }),
}));

// Mocked: starting a run is the launcher's job; the tests only check what the selection asks it to run.
vi.mock('./useRunLauncher', () => ({
  useRunLauncher: () => ({
    phase: 'idle',
    request: null,
    startError: null,
    otherRunInProgress: false,
    busy: false,
    start: vi.fn(),
  }),
}));

// the outcome of the latest run is the shell's banner; these tests are about the selection
vi.mock('../shell/useRunBannerViewModel', () => ({
  useRunBannerViewModel: () => ({
    banner: { visible: false, variant: 'running', source: 'config', dismissible: false, copy: { titleKey: '', bodyKey: '' } },
    title: '',
    body: '',
    line: '',
    dismiss: null,
    primaryActionLabel: '',
    dismissLabel: 'Dismiss',
    onPrimaryAction: null,
  }),
}));

describe(useRerunViewModel.name, () => {
  const wrapper: ComponentType<{ children: ReactNode }> = ({ children }) => (
    <RerunSelectionProvider>{children}</RerunSelectionProvider>
  );

  it('marks the dimensions no completed run has measured yet', () => {
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    expect(result.current.isDimensionNotMeasured('files')).toBe(false);
    expect(result.current.isDimensionNotMeasured('asset-hierarchy')).toBe(true);
  });

  it('marks nothing while it is not known what the runs measured', () => {
    dimensionRuns = undefined;
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    expect(result.current.isDimensionNotMeasured('asset-hierarchy')).toBe(false);
    dimensionRuns = { files: '2026-11-04T06:00:00.000Z' };
  });

  it('starts with nothing selected and cannot run', () => {
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    expect(result.current.selectedTotal).toBe(0);
    expect(result.current.canRun).toBe(false);
    expect(result.current.request).toBeNull();
  });

  it('builds a request from mixed dimension and metric picks', () => {
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    act(() => {
      result.current.toggleDimension('files');
      result.current.toggleMetric('a:B:v1|Asset.name');
    });

    expect(result.current.selectedTotal).toBe(2);
    expect(result.current.canRun).toBe(true);
    expect(result.current.request).toEqual({
      kind: 'selection',
      dimensionIds: ['files'],
      links: [{ model: 'a:B:v1', link: 'Asset.name' }],
    });
  });

  it('keeps picks when the tab changes', () => {
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    act(() => {
      result.current.toggleDimension('files');
      result.current.setTab('metrics');
    });

    expect(result.current.tab).toBe('metrics');
    expect(result.current.selectedDimensions).toEqual(['files']);
  });

  it('select all metrics only affects the filtered data model', () => {
    const { result } = renderHook(() => useRerunViewModel(), { wrapper });

    act(() => {
      result.current.setMetricsModelFilter('c:D:v2');
    });
    act(() => {
      result.current.toggleAllMetrics();
    });

    expect(result.current.selectedMetrics.map((metric) => metric.id)).toEqual(['c:D:v2|Tank.level']);
    expect(result.current.isMetricSelected('a:B:v1|Asset.name')).toBe(false);
  });
});
