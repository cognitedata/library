import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import en from '../../i18n/en.json';
import {
  FakeRecordsClient,
  RUN_NEW,
  aggregateRow,
  drilldownRow,
  runRow,
} from '../../services/cdf/testSupport';
import { createCdfServices, type AppServices } from '../../services/createServices';
import { createMockServices } from '../../services/mock/createMockServices';
import { makeMockHostApi } from '../../test/mockHostApi';
import { renderOverview } from '../../test/renderOverview';

const messages: Record<string, string> = en;
const STREAM = { streamId: 'cq_stream', schemaSpace: 'cq_schema', relationshipContainer: 'Run', windowDays: null };
const RUN_TS = '2026-10-12T06:00:00.000Z';

type Row = Readonly<Record<string, unknown>>;

/** A fake Records API in front of the real CDF services: routes the stream lookup and the records filter. */
function backendFetch(options: {
  rows?: readonly Row[];
  streamLookup?: Response;
  filter?: Response;
  throws?: boolean;
}): typeof fetch {
  const client = new FakeRecordsClient([...(options.rows ?? [])]);
  return async (input, init) => {
    if (options.throws) {
      throw new TypeError('Failed to fetch');
    }
    const url = String(input);
    if (!url.endsWith('/records/filter')) {
      return options.streamLookup ?? Response.json({ settings: { limits: { maxFilteringInterval: 'PT168H' } } });
    }
    if (options.filter) {
      return options.filter;
    }
    const body: unknown = JSON.parse(String(init?.body));
    const clauses =
      typeof body === 'object' && body !== null && 'filter' in body ? (body.filter as { and: { equals: { property: string[]; value: string | number } }[] }).and : [];
    const equals = Object.fromEntries(clauses.map((clause) => [clause.equals.property[2] ?? '', clause.equals.value]));
    const page = await client.query({ equals });
    return Response.json({
      items: page.rows.map((row) => ({ properties: { [STREAM.schemaSpace]: { [STREAM.relationshipContainer]: row } } })),
    });
  };
}

function cdfServices(fetchFn: typeof fetch): AppServices {
  return createCdfServices({ stream: STREAM, workflow: null, getHostApi: () => makeMockHostApi(), fetchFn, now: () => Date.parse(RUN_TS) });
}

function renderShell(services: AppServices, state: Record<string, unknown> = {}) {
  return renderOverview(<ProductShell hostInitialState={JSON.stringify(state)} />, { services });
}

function apiError(status: number, message: string): Response {
  return new Response(JSON.stringify({ error: { code: status, message } }), { status });
}

const HEALTHY_ROWS: Row[] = [
  runRow(RUN_NEW, RUN_TS),
  aggregateRow({ metricId: 'hierarchyCompletionRate', dimensionId: 'asset-hierarchy', eligible: 10, passing: 9 }),
  aggregateRow({ metricId: 'orphanRate', dimensionId: 'asset-hierarchy', eligible: 10, passing: 8 }),
  drilldownRow('orphansByTier', 'asset-hierarchy', 'a-1'),
];

describe('what the dashboard shows when the backend cannot be read', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it.each([
    ['access is denied', { filter: apiError(403, 'Forbidden') }, 'permission-denied'],
    ['the session expired', { filter: apiError(401, 'Unauthorized') }, 'unauthenticated'],
    [
      'the stream does not exist',
      { streamLookup: apiError(404, 'Stream with externalId cq_stream not found'), filter: apiError(400, 'Stream cq_stream not found') },
      'stream-missing',
    ],
    ['the schema is missing', { filter: apiError(400, 'Unknown space: cq_schema') }, 'schema-missing'],
    ['the API fails', { filter: apiError(500, 'boom') }, 'request-failed'],
    ['the request never arrives', { throws: true }, 'network'],
  ])('explains it when %s', async (_name, options, kind) => {
    renderShell(cdfServices(backendFetch(options)));

    expect(await screen.findByTestId('backend-error')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: messages[`backend.error.${kind}.title`] })).toBeInTheDocument();
    expect(screen.queryByTestId('demo-mode-banner')).not.toBeInTheDocument();
  });

  it('says the stream is empty when no run has ever been written', async () => {
    renderShell(cdfServices(backendFetch({ rows: [] })), { page: 'configuration' });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.getByText(messages['configuration.firstRunOnboarding.welcome.title'] ?? '')).toBeInTheDocument();
  });

  it('shows first-run onboarding on Configuration without page tabs when the stream is empty', async () => {
    renderShell(cdfServices(backendFetch({ rows: [] })), { page: 'configuration' });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByRole('tab', { name: 'Overview' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
    expect(screen.queryByTestId('backend-error')).not.toBeInTheDocument();
  });

  it('shows first-run onboarding when only unfinished runs exist', async () => {
    renderShell(cdfServices(backendFetch({ rows: [runRow('run-x', RUN_TS, 'started')] })));

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByTestId('backend-error-configuration')).not.toBeInTheDocument();
  });

  it('does not send a missing backend to Configuration', async () => {
    renderShell(cdfServices(backendFetch({ streamLookup: apiError(404, 'Stream with externalId cq_stream not found'), filter: apiError(400, 'Stream cq_stream not found') })));

    expect(await screen.findByTestId('backend-error')).toBeInTheDocument();
    expect(screen.queryByTestId('backend-error-configuration')).not.toBeInTheDocument();
  });

  it('shows onboarding for any host page state while no run has completed', async () => {
    renderShell(cdfServices(backendFetch({ rows: [] })), { page: 'configuration' });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByTestId('backend-error')).not.toBeInTheDocument();
    expect(screen.queryByText('Upload custom configuration file')).not.toBeInTheDocument();
  });

  it('shows the no-run panel on Overview until onboarding is on Configuration', async () => {
    renderShell(cdfServices(backendFetch({ rows: [] })), { page: 'overview' });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
    expect(screen.queryByTestId('backend-error')).not.toBeInTheDocument();
  });

  it('shows onboarding when only unfinished runs exist regardless of host page', async () => {
    renderShell(cdfServices(backendFetch({ rows: [runRow('run-x', RUN_TS, 'started')] })), { page: 'configuration' });

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
  });

  it('shows regular Configuration after onboarding is marked complete', async () => {
    renderShell(cdfServices(backendFetch({ rows: [] })), {
      page: 'configuration',
      configurationOnboardingComplete: true,
    });

    expect(await screen.findByTestId('run-config-editor-card')).toBeInTheDocument();
    expect(screen.queryByTestId('first-run-onboarding')).not.toBeInTheDocument();
  });

  it('keeps the error in place of the Configuration page for any failure other than having no run yet', async () => {
    renderShell(cdfServices(backendFetch({ filter: apiError(403, 'Forbidden') })), { page: 'configuration' });

    expect(await screen.findByTestId('backend-error')).toBeInTheDocument();
    expect(screen.queryByText('Upload custom configuration file')).not.toBeInTheDocument();
  });

  it('shows onboarding when only unfinished runs exist', async () => {
    renderShell(cdfServices(backendFetch({ rows: [runRow('run-x', RUN_TS, 'started')] })));

    expect(await screen.findByTestId('first-run-onboarding')).toBeInTheDocument();
  });

  it('never shows sample data in place of a backend that cannot be read', async () => {
    renderShell(cdfServices(backendFetch({ filter: apiError(403, 'Forbidden') })));

    await screen.findByTestId('backend-error');

    expect(screen.queryByText(/Demo (North|Central|South)/)).not.toBeInTheDocument();
  });

  it('retries after the cause has been fixed', async () => {
    let allowed = false;
    const healthy = backendFetch({ rows: HEALTHY_ROWS });
    const fetchFn: typeof fetch = async (input, init) => (allowed ? healthy(input, init) : apiError(403, 'Forbidden'));
    renderShell(cdfServices(fetchFn));
    await screen.findByTestId('backend-error');

    allowed = true;
    await userEvent.setup().click(screen.getByTestId('backend-error-retry'));

    await waitFor(() => {
      expect(screen.queryByTestId('backend-error')).not.toBeInTheDocument();
    });
  });
});

describe('demo mode', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('is announced on every page while the sample data is in use', async () => {
    renderShell(createMockServices({ latency: 0 }));

    expect(await screen.findByTestId('demo-mode-banner')).toHaveTextContent(messages['backend.demo.banner'] ?? '');
  });

  it('is not shown for a deployed backend', async () => {
    renderShell(cdfServices(backendFetch({ rows: HEALTHY_ROWS })));

    await screen.findByRole('tab', { name: 'Overview', selected: true });

    expect(screen.queryByTestId('demo-mode-banner')).not.toBeInTheDocument();
  });
});

describe('features the deployed backend does not provide', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('shows the alerts page as unavailable, not as an empty list of alerts', async () => {
    renderShell(cdfServices(backendFetch({ rows: HEALTHY_ROWS })), { page: 'alerts' });

    expect(await screen.findByTestId('backend-unavailable')).toHaveTextContent(messages['backend.unavailable.alerts'] ?? '');
    expect(screen.queryByTestId('alerts-empty')).not.toBeInTheDocument();
  });

  it('still shows alerts on the demo data', async () => {
    renderShell(createMockServices({ latency: 0 }), { page: 'alerts' });

    await waitFor(() => {
      expect(screen.queryByTestId('backend-unavailable')).not.toBeInTheDocument();
    });
    expect(await screen.findByText('Alerts')).toBeInTheDocument();
  });

  it('offers no rerun and no notify-owner action for a deployed backend', async () => {
    renderShell(cdfServices(backendFetch({ rows: HEALTHY_ROWS })), {
      page: 'dimensions',
      dimensionId: 'asset-hierarchy',
      metricId: 'orphansByTier',
    });

    await screen.findByTestId('drilldown-table');

    expect(screen.queryByTestId('rerun-dimension')).not.toBeInTheDocument();
    expect(screen.queryByTestId('rerun-metric')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Notify owner' })).not.toBeInTheDocument();
  });

  it('keeps rerun and notify-owner on the demo data', async () => {
    renderShell(createMockServices({ latency: 0 }), {
      page: 'dimensions',
      dimensionId: 'asset-hierarchy',
      metricId: 'orphansByTier',
    });

    expect(await screen.findByTestId('rerun-dimension')).toBeInTheDocument();
    expect(await screen.findByTestId('rerun-metric')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Notify owner' })).toBeInTheDocument();
  });

  it('shows run configuration only when settings editing is unavailable', async () => {
    renderShell(cdfServices(backendFetch({ rows: HEALTHY_ROWS })), { page: 'configuration' });

    expect(await screen.findByTestId('run-config-editor-card')).toBeInTheDocument();
    expect(screen.queryByTestId('feature-flags-card')).not.toBeInTheDocument();
    expect(screen.queryByTestId('add-exclusion-rule')).not.toBeInTheDocument();
  });

  it('shows no owner mappings, exclusion rules or recipients it did not get from the backend', async () => {
    renderShell(cdfServices(backendFetch({ rows: HEALTHY_ROWS })), { page: 'configuration' });

    await screen.findByTestId('run-config-editor-card');

    expect(screen.queryByText('PI-COMP3-')).not.toBeInTheDocument();
    expect(screen.queryByText(/example\.test/)).not.toBeInTheDocument();
  });
});
