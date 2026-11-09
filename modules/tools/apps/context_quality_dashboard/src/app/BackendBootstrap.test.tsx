import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { resolveBackendConfig, type BackendConfigResult } from '../services/cdf/streamConfig';
import { createCdfServices, type loadServices } from '../services/createServices';
import { createMockServices } from '../services/mock/createMockServices';
import { useServices } from '../services/useServices';

import { BackendBootstrap } from './BackendBootstrap';

const cdfEnv = {
  VITE_QUALITY_BACKEND: 'cdf',
  VITE_CQ_STREAM_ID: 'cq_stream',
  VITE_CQ_SCHEMA_SPACE: 'cq_schema',
};

type Loader = typeof loadServices;

function Probe() {
  return <p data-testid="probe">{useServices().backend.kind}</p>;
}

function renderBootstrap(config: BackendConfigResult, load: Loader) {
  return render(
    <BackendBootstrap config={config} load={load}>
      <Probe />
    </BackendBootstrap>
  );
}

describe(BackendBootstrap.name, () => {
  it('shows what to set, and loads nothing, when no backend is selected', () => {
    const load = vi.fn<Loader>();

    renderBootstrap(resolveBackendConfig({}), load);

    expect(screen.getByTestId('backend-config-error')).toBeInTheDocument();
    expect(screen.getByText('VITE_QUALITY_BACKEND')).toBeInTheDocument();
    expect(screen.queryByTestId('probe')).not.toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });

  it('lists every missing resource setting for a cdf selection', () => {
    const load = vi.fn<Loader>();

    renderBootstrap(resolveBackendConfig({ VITE_QUALITY_BACKEND: 'cdf' }), load);

    expect(screen.getByText('VITE_CQ_STREAM_ID')).toBeInTheDocument();
    expect(screen.getByText('VITE_CQ_SCHEMA_SPACE')).toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });

  it('never falls back to the sample data when the configuration is invalid', () => {
    const load = vi.fn<Loader>(async () => createMockServices({ latency: 0 }));

    renderBootstrap(resolveBackendConfig({ VITE_QUALITY_BACKEND: 'demo' }), load);

    expect(screen.queryByTestId('probe')).not.toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });

  it('builds the services for a cdf configuration and renders the app', async () => {
    const config = resolveBackendConfig(cdfEnv);
    const load = vi.fn<Loader>(async () =>
      createCdfServices({
        stream: { streamId: 'cq_stream', schemaSpace: 'cq_schema', relationshipContainer: 'C', windowDays: null },
        workflow: null,
        getHostApi: () => null,
        fetchFn: vi.fn<typeof fetch>(),
        now: () => 0,
      })
    );

    renderBootstrap(config, load);

    await waitFor(() => {
      expect(screen.getByTestId('probe')).toHaveTextContent('cdf');
    });
    expect(load).toHaveBeenCalledWith(config.ok ? config.config : undefined);
  });

  it('renders the app on sample data only when the mock is selected explicitly', async () => {
    const load = vi.fn<Loader>(async () => createMockServices({ latency: 0 }));

    renderBootstrap(resolveBackendConfig({ VITE_QUALITY_BACKEND: 'mock' }), load);

    await waitFor(() => {
      expect(screen.getByTestId('probe')).toHaveTextContent('mock');
    });
  });

  it('explains a failure to build the services instead of rendering the app', async () => {
    const load = vi.fn<Loader>(async () => {
      throw new Error('chunk failed to load');
    });

    renderBootstrap(resolveBackendConfig(cdfEnv), load);

    expect(await screen.findByTestId('backend-error')).toBeInTheDocument();
    expect(screen.getByTestId('backend-error-details')).toHaveTextContent('chunk failed to load');
    expect(screen.queryByTestId('probe')).not.toBeInTheDocument();
  });
});
