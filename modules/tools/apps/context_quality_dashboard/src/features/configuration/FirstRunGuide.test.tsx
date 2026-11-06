import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { BackendError } from '../../services/cdf/backendErrors';
import type { WorkflowConfig } from '../../services/cdf/streamConfig';
import { FakeRecordsClient } from '../../services/cdf/testSupport';
import { createCdfServices, type AppServices } from '../../services/createServices';
import { createMockServices } from '../../services/mock/createMockServices';
import { makeMockHostApi } from '../../test/mockHostApi';
import { renderOverview } from '../../test/renderOverview';

import { RunConfigFileContext } from './runConfigFileContext';
import { createRunConfigFileStore } from './runConfigFileStore';

const STREAM = { streamId: 'cq_stream', schemaSpace: 'cq_schema', relationshipContainer: 'Run', windowDays: null };
const RUN_TS = '2026-10-12T06:00:00.000Z';

type Row = Readonly<Record<string, unknown>>;

function backendFetch(rows: readonly Row[]): typeof fetch {
  const client = new FakeRecordsClient([...rows]);
  return async (input, init) => {
    const url = String(input);
    if (!url.endsWith('/records/filter')) {
      return Response.json({ settings: { limits: { maxFilteringInterval: 'PT168H' } } });
    }
    const body: unknown = JSON.parse(String(init?.body));
    const clauses =
      typeof body === 'object' && body !== null && 'filter' in body
        ? (body.filter as { and: { equals: { property: string[]; value: string | number } }[] }).and
        : [];
    const equals = Object.fromEntries(clauses.map((clause) => [clause.equals.property[2] ?? '', clause.equals.value]));
    const page = await client.query({ equals });
    return Response.json({
      items: page.rows.map((row) => ({ properties: { [STREAM.schemaSpace]: { [STREAM.relationshipContainer]: row } } })),
    });
  };
}

function cdfServices(
  fetchFn: typeof fetch,
  workflow: WorkflowConfig | null = { externalId: 'wf', version: 'v1' }
): AppServices {
  return createCdfServices({
    stream: STREAM,
    workflow,
    getHostApi: () => makeMockHostApi(),
    fetchFn,
    now: () => Date.parse(RUN_TS),
  });
}

function renderConfiguration(latest: { runTimestamp: string; models: never[] } | null, canRun = true) {
  const services = createMockServices({ latency: 0 });
  vi.spyOn(services.qualityDashboardService, 'getLatestRunConfiguration').mockResolvedValue(latest);
  if (latest !== null) {
    vi.spyOn(services.qualityDashboardService, 'getRunStatus').mockResolvedValue({
      lastSuccessAt: latest.runTimestamp,
      inProgress: false,
      scope: { kind: 'all' },
    });
  } else {
    vi.spyOn(services.qualityDashboardService, 'getRunStatus').mockRejectedValue(
      new BackendError('no-completed-run', 'empty', { detail: 'empty' })
    );
  }
  return renderOverview(
    <RunConfigFileContext.Provider value={{ store: createRunConfigFileStore(null), pollMs: 5 }}>
      <ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />
    </RunConfigFileContext.Provider>,
    { services: canRun ? services : { ...services, runService: null } }
  );
}

describe('first-run guide on the Configuration page', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('does not show the legacy guide before any run; that state uses first-run onboarding in the shell instead', async () => {
    renderConfiguration(null);

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByTestId('first-run-guide')).not.toBeInTheDocument();
  });

  it('explains configured metrics when the latest run measured only the built-in checks', async () => {
    renderConfiguration({ runTimestamp: '2026-10-15T17:45:27.000Z', models: [] });

    expect(await screen.findByTestId('first-run-guide')).toBeInTheDocument();
    expect(screen.getByTestId('first-run-guide-built-in-only')).toHaveTextContent('Custom configured metrics');
    expect(screen.queryByTestId('first-run-guide-no-run')).not.toBeInTheDocument();
  });

  it('is not shown once a run has used a configuration file', async () => {
    renderConfiguration({
      runTimestamp: '2026-10-15T17:45:27.000Z',
      models: [{ model: 'cdf_cdm:CogniteCore:v1', views: [{ view: 'CogniteAsset', properties: ['type'] }] }] as never[],
    });

    await screen.findByText('Upload custom configuration file');
    expect(screen.queryByTestId('first-run-guide')).not.toBeInTheDocument();
  });
});

describe('first-run onboarding page', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('does not start a run by itself on first visit, and starts the built-in checks from the button', async () => {
    const user = userEvent.setup();
    const services = cdfServices(backendFetch([]));
    if (services.runService === null) {
      throw new Error('Expected run service');
    }
    const start = vi.spyOn(services.runService, 'start');

    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, { services });

    await screen.findByTestId('first-run-onboarding');
    expect(start).not.toHaveBeenCalled();

    await user.click(await screen.findByTestId('first-run-built-in-run'));

    await waitFor(() => {
      expect(start).toHaveBeenCalledWith({
        extraLinks: [],
        scope: { dimensions: 'all', configured: false },
      });
    });
  });

  it('starts a built-in run with empty extra links on first visit when the build enables auto-start', async () => {
    vi.stubEnv('VITE_CQ_AUTO_START_FIRST_RUN', 'true');
    try {
      const services = cdfServices(backendFetch([]));
      if (services.runService === null) {
        throw new Error('Expected run service');
      }
      const start = vi.spyOn(services.runService, 'start');

      renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, { services });

      await screen.findByTestId('first-run-onboarding');

      await waitFor(() => {
        expect(start).toHaveBeenCalledWith({
          extraLinks: [],
          scope: { dimensions: 'all', configured: false },
        });
      });
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it('hides page tabs while onboarding is shown', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([])),
    });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByRole('tab', { name: 'Overview' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { level: 1, name: 'Contextualization Quality' })
    ).toBeInTheDocument();
  });

  it('shows onboarding in dev when the preview flag is set', async () => {
    vi.stubEnv('DEV', true);
    vi.stubEnv('VITE_DEV_FORCE_FIRST_RUN_ONBOARDING', 'true');
    try {
      renderOverview(<ProductShell />, { services: createMockServices({ latency: 0 }) });
      expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
      expect(screen.queryByTestId('demo-mode-banner')).not.toBeInTheDocument();
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it('shows the example YAML help banner before a configuration file is uploaded', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([])),
    });

    await screen.findByTestId('first-run-onboarding');
    expect(screen.getByTestId('run-config-example-card')).toBeInTheDocument();
  });

  it('hides the example YAML help banner while a configuration file is present', async () => {
    const yaml = `- model: cdf_cdm:CogniteCore:v1
  links:
    - CogniteAsset.type
`;
    const store = createRunConfigFileStore(null);
    store.save({ fileName: 'sample.yaml', text: yaml });
    renderOverview(
      <RunConfigFileContext.Provider value={{ store, pollMs: 5 }}>
        <ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />
      </RunConfigFileContext.Provider>,
      { services: cdfServices(backendFetch([])) }
    );

    await screen.findByTestId('first-run-onboarding');
    expect(screen.queryByTestId('run-config-example-card')).not.toBeInTheDocument();
  });

  it('shows the example YAML help banner again after the uploaded file is removed', async () => {
    const user = userEvent.setup();
    const yaml = `- model: cdf_cdm:CogniteCore:v1
  links:
    - CogniteAsset.type
`;
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([])),
    });

    await screen.findByTestId('first-run-onboarding');
    expect(screen.getByTestId('run-config-example-card')).toBeInTheDocument();

    const input = await screen.findByTestId('run-config-file-input');
    await user.upload(input, new File([yaml], 'sample.yaml', { type: 'text/yaml' }));

    await screen.findByTestId('run-config-uploaded-file');
    expect(screen.queryByTestId('run-config-example-card')).not.toBeInTheDocument();

    if (screen.queryByTestId('run-config-review-dialog') !== null) {
      await user.keyboard('{Escape}');
    }
    await user.click(screen.getByRole('button', { name: 'Remove file' }));

    await waitFor(() => {
      expect(screen.getByTestId('run-config-example-card')).toBeInTheDocument();
    });
  });

  it('shows Skipped on the optional card and a Configuration tab footnote when Skip for now is chosen', async () => {
    const user = userEvent.setup();
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([])),
    });

    await screen.findByTestId('first-run-onboarding');
    await user.click(screen.getByTestId('first-run-skip'));

    expect(await screen.findByTestId('first-run-configured-skipped-badge')).toHaveTextContent('Skipped');
    expect(screen.getByTestId('first-run-configured-skipped-footnote')).toHaveTextContent(
      'Configuration tab'
    );
    expect(screen.queryByTestId('run-config-upload-dropzone')).not.toBeInTheDocument();
    expect(screen.getByTestId('first-run-onboarding')).toBeInTheDocument();
  });

  it('shows the standard Configuration page after Continue to Overview following skip', async () => {
    const user = userEvent.setup();
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([])),
    });

    await screen.findByTestId('first-run-onboarding');
    await user.click(screen.getByTestId('first-run-skip'));
    await screen.findByTestId('first-run-configured-skipped-badge');

    await user.click(screen.getByRole('button', { name: 'Continue to Overview' }));
    await waitFor(() => {
      expect(screen.queryByTestId('first-run-onboarding')).not.toBeInTheDocument();
    });
    expect(screen.getByRole('tab', { name: 'Overview', selected: true })).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByRole('heading', { name: 'Upload custom configuration file' })).toBeInTheDocument();
    expect(screen.queryByTestId('first-run-onboarding')).not.toBeInTheDocument();
  });

  it('says plainly that runs cannot start when this build has no workflow', async () => {
    renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'configuration' })} />, {
      services: cdfServices(backendFetch([]), null),
    });

    expect(await screen.findByTestId('first-run-built-in-status')).toHaveTextContent(
      'uploading a file here will not start anything'
    );
    expect(screen.queryByTestId('first-run-run-built-in')).not.toBeInTheDocument();
  });
});
