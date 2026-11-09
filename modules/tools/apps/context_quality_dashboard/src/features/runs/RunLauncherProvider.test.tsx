import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { createMockServices } from '../../services/mock/createMockServices';
import type { RunService } from '../../services/RunService';
import { renderOverview } from '../../test/renderOverview';
import { RunConfigFileContext } from '../configuration/runConfigFileContext';
import { createRunConfigFileStore } from '../configuration/runConfigFileStore';

import { RunLauncherProvider } from './RunLauncherProvider';
import { useRunLauncher } from './useRunLauncher';

describe(RunLauncherProvider.name, () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('starts the run it is asked for, follows it, and reads the new run when it has finished', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['running', 'completed']);
    const services = { ...createMockServices({ latency: 0 }), runService };
    const refresh = vi.spyOn(services.qualityDashboardService, 'refresh');
    renderLauncher(services);

    await user.click(screen.getByRole('button', { name: 'start files' }));

    expect(runService.start).toHaveBeenCalledWith({
      extraLinks: [],
      scope: { dimensions: ['files'], configured: false },
    });
    await waitFor(() => expect(screen.getByTestId('phase')).toHaveTextContent('running'));
    await waitFor(() => expect(screen.getByTestId('phase')).toHaveTextContent('completed'));
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));
    expect(screen.getByTestId('request')).toHaveTextContent('dimension-files');
  });

  it('is busy, and says the run is not its own, while a run it did not start is going on', async () => {
    const runService = runServiceWith(['completed'], true);
    renderLauncher({ ...createMockServices({ latency: 0 }), runService });

    await waitFor(() => expect(screen.getByTestId('other')).toHaveTextContent('true'));
    expect(screen.getByTestId('busy')).toHaveTextContent('true');
    expect(screen.getByTestId('phase')).toHaveTextContent('idle');
  });

  it('is not busy when no run is going on', async () => {
    renderLauncher({ ...createMockServices({ latency: 0 }), runService: runServiceWith(['completed']) });

    await waitFor(() => expect(screen.getByTestId('busy')).toHaveTextContent('false'));
  });

  it('reports a run that could not be started', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    runService.start.mockRejectedValue(new Error('boom'));
    renderLauncher({ ...createMockServices({ latency: 0 }), runService });

    await user.click(screen.getByRole('button', { name: 'start files' }));

    await waitFor(() => expect(screen.getByTestId('phase')).toHaveTextContent('not-started'));
    expect(screen.getByTestId('error')).toHaveTextContent('boom');
  });

  it('offers nothing when the deployment has no workflow to run', () => {
    renderLauncher({ ...createMockServices({ latency: 0 }), runService: null });

    expect(screen.getByTestId('available')).toHaveTextContent('false');
  });
});

function renderLauncher(services: ReturnType<typeof createMockServices>) {
  return renderOverview(
    <RunConfigFileContext.Provider value={{ store: createRunConfigFileStore(null), pollMs: 5 }}>
      <RunLauncherProvider runningCheckMs={5}>
        <Probe />
      </RunLauncherProvider>
    </RunConfigFileContext.Provider>,
    { services }
  );
}

function Probe() {
  const launcher = useRunLauncher();
  return (
    <div>
      <span data-testid="available">{String(launcher !== null)}</span>
      <span data-testid="phase">{launcher?.phase ?? 'none'}</span>
      <span data-testid="busy">{String(launcher?.busy ?? false)}</span>
      <span data-testid="other">{String(launcher?.otherRunInProgress ?? false)}</span>
      <span data-testid="request">{launcher?.request === null || launcher === null ? '' : `${launcher.request.kind}-${launcher.request.kind === 'dimension' ? launcher.request.dimensionId : ''}`}</span>
      <span data-testid="error">{launcher?.startError?.message ?? ''}</span>
      <button type="button" onClick={() => launcher?.start({ kind: 'dimension', dimensionId: 'files' }, null)}>
        start files
      </button>
    </div>
  );
}

function runServiceWith(statuses: ('running' | 'completed' | 'failed')[], elsewhere = false): RunService & {
  start: ReturnType<typeof vi.fn>;
} {
  let polled = 0;
  return {
    start: vi.fn(async () => ({ executionId: 'exec-1' })),
    getStatus: vi.fn(async () => statuses[Math.min(polled++, statuses.length - 1)] ?? 'failed'),
    findRunning: vi.fn(async () => elsewhere),
  };
}
