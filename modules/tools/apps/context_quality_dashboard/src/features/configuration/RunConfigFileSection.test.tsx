import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { BackendError } from '../../services/cdf/backendErrors';
import { createMockServices } from '../../services/mock/createMockServices';
import type { RunService } from '../../services/RunService';
import { renderOverview } from '../../test/renderOverview';

import { RunConfigFileContext } from './runConfigFileContext';
import * as runConfigFileExample from './runConfigFileExample';
import { createRunConfigFileStore, type RunConfigFileStore } from './runConfigFileStore';

const VALID = `- model: cdf_cdm:CogniteCore:v1
  links:
    - CogniteAsset.type
    - CogniteEquipment.equipmentType
- model: my_space:MyModel:v2
  links:
    - MyView.owner
`;

const LATEST_RUN_MATCHING_VALID = {
  runTimestamp: '2026-10-15T17:45:27.000Z',
  models: [
    {
      model: 'cdf_cdm:CogniteCore:v1',
      views: [
        { view: 'CogniteAsset', properties: ['type'] },
        { view: 'CogniteEquipment', properties: ['equipmentType'] },
      ],
    },
    { model: 'my_space:MyModel:v2', views: [{ view: 'MyView', properties: ['owner'] }] },
  ],
};

const RUN_CUSTOM_CONFIG_LABEL = 'Run custom configuration file';

describe('Rerun configuration', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('cannot start a run when the deployment has no workflow, and says so', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), null);
    await dismissReviewDialog(user);

    await user.click(await screen.findByRole('checkbox', { name: 'Files' }));
    expect(await screen.findByRole('button', { name: 'Run selected (1)' })).toBeDisabled();
    expect(screen.getByText(/No workflow is deployed/)).toBeInTheDocument();
  });

  it('has a disabled run button while nothing is selected', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), runServiceWith(['completed']));
    await dismissReviewDialog(user);

    expect(screen.getByRole('button', { name: 'Run selected' })).toBeDisabled();
  });

  it('no longer offers the old run buttons or an everything tab', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), runServiceWith(['completed']));
    await dismissReviewDialog(user);

    await screen.findByTestId('rerun-configuration');
    expect(screen.queryByRole('button', { name: 'Run everything' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Run all dimensions' })).not.toBeInTheDocument();
    expect(screen.queryByText('What would you like to run?')).not.toBeInTheDocument();
  });

  it('runs picked default dimensions without a configuration file', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    renderConfiguration(createRunConfigFileStore(null), runService);

    await user.click(await screen.findByRole('checkbox', { name: 'Files' }));
    await user.click(screen.getByRole('checkbox', { name: 'Asset Hierarchy' }));
    await user.click(screen.getByRole('button', { name: 'Run selected (2)' }));
    await user.click(await screen.findByTestId('confirm-run-start-selection'));

    expect(runService.start).toHaveBeenCalledWith({
      extraLinks: [],
      scope: { dimensions: ['asset-hierarchy', 'files'], configured: false },
    });
  });

  it('selects all default dimensions and shows the parent as indeterminate for a partial pick', async () => {
    const user = userEvent.setup();
    renderConfiguration(createRunConfigFileStore(null), runServiceWith(['completed']));
    const all = await screen.findByRole('checkbox', { name: 'Select all default dimensions' });

    await user.click(screen.getByRole('checkbox', { name: 'Files' }));
    expect(all).toHaveAttribute('aria-checked', 'mixed');

    await user.click(all);
    expect(all).toHaveAttribute('aria-checked', 'true');
    expect(screen.getByRole('button', { name: 'Run selected (7)' })).toBeEnabled();

    await user.click(all);
    expect(all).toHaveAttribute('aria-checked', 'false');
  });

  it('groups configured metrics by model and view with selected counts, and selects a whole view', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), runServiceWith(['completed']));
    await dismissReviewDialog(user);
    await user.click(await screen.findByRole('tab', { name: /Custom configured metrics/ }));

    const model = await screen.findByTestId('rerun-model-cdf_cdm:CogniteCore:v1');
    expect(within(model).getByText('CogniteAsset')).toBeInTheDocument();
    expect(screen.getByTestId('rerun-view-count-cdf_cdm:CogniteCore:v1|CogniteAsset')).toHaveTextContent('0 / 1');

    await user.click(screen.getByTestId('rerun-view-checkbox-cdf_cdm:CogniteCore:v1|CogniteAsset'));

    expect(screen.getByTestId('rerun-view-count-cdf_cdm:CogniteCore:v1|CogniteAsset')).toHaveTextContent('1 / 1');
    expect(screen.getByTestId('rerun-select-all-metrics')).toHaveAttribute('aria-checked', 'mixed');
    expect(screen.getByTestId('rerun-summary-metrics')).toHaveTextContent('1 of 3 selected');
    expect(screen.getByTestId('rerun-summary-metrics')).not.toHaveTextContent('CogniteAsset');
  });

  it('keeps picks when switching tabs, and runs both groups together with only the picked links', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    renderConfiguration(storeWith(VALID), runService);
    await dismissReviewDialog(user);

    await user.click(await screen.findByRole('checkbox', { name: 'Files' }));
    await user.click(screen.getByRole('tab', { name: /Custom configured metrics/ }));
    await user.click(await screen.findByRole('checkbox', { name: 'owner' }));
    await user.click(screen.getByRole('tab', { name: /Default dimensions/ }));

    expect(screen.getByRole('checkbox', { name: 'Files' })).toHaveAttribute('aria-checked', 'true');
    expect(screen.getByRole('tab', { name: 'Custom configured metrics' })).toBeInTheDocument();
    expect(screen.getByTestId('rerun-summary-total')).toHaveTextContent('2 items selected');

    await user.click(screen.getByRole('button', { name: 'Run selected (2)' }));
    await user.click(await screen.findByTestId('confirm-run-start-selection'));

    expect(runService.start).toHaveBeenCalledTimes(1);
    expect(runService.start).toHaveBeenCalledWith({
      extraLinks: [{ model: 'my_space:MyModel:v2', links: ['MyView.owner'] }],
      scope: { dimensions: ['files'], configured: true },
    });
  });

  it('shows an empty state for metrics without a valid configuration, and keeps dimensions usable', async () => {
    const user = userEvent.setup();
    renderConfiguration(createRunConfigFileStore(null), runServiceWith(['completed']));

    await user.click(await screen.findByRole('tab', { name: /Custom configured metrics/ }));
    expect(await screen.findByTestId('rerun-metrics-empty')).toHaveTextContent('No valid configuration');

    await user.click(screen.getByRole('tab', { name: /Default dimensions/ }));
    await user.click(screen.getByRole('checkbox', { name: 'Files' }));
    expect(screen.getByRole('button', { name: 'Run selected (1)' })).toBeEnabled();
  });

  it('starts nothing when the confirmation is cancelled', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    renderConfiguration(storeWith(VALID), runService);
    await dismissReviewDialog(user);

    await user.click(await screen.findByRole('checkbox', { name: 'Files' }));
    await user.click(screen.getByRole('button', { name: 'Run selected (1)' }));
    await user.click(await screen.findByTestId('confirm-run-cancel-selection'));

    expect(runService.start).not.toHaveBeenCalled();
  });

  it('follows the run, blocks a second submission while it runs, and says when it has finished', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['running', 'running', 'completed']);
    renderConfiguration(storeWith(VALID), runService);

    await clickRun(user);

    expect(await screen.findByTestId('run-status-banner-running')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Running' })).toBeDisabled()
    );
    expect(runService.start).toHaveBeenCalledTimes(1);
    expect(await screen.findByTestId('run-status-banner-success')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Run selected (1)' })).toBeEnabled()
    );
  });

  it('shows a progress bar while the run is in progress', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), runServiceWith(['running']));

    await clickRun(user);

    await screen.findByTestId('run-status-banner-running');
    expect(screen.getByTestId('run-config-progress')).toBeInTheDocument();
  });

  it('says the run failed and lists the items it asked for', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID), runServiceWith(['running', 'failed']));

    await clickRun(user);

    expect(await screen.findByTestId('run-status-banner-failed')).toBeInTheDocument();
    expect(screen.getByTestId('run-status')).toHaveTextContent('The run failed. Nothing was added to the Overview.');
    expect(await screen.findByTestId('rerun-failed-items')).toHaveTextContent('Files');
  });

  it('says the user lacks permission when starting the run is refused', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    runService.start.mockRejectedValue(new BackendError('permission-denied', 'no', { status: 403 }));
    renderConfiguration(storeWith(VALID), runService);

    await clickRun(user);

    expect(await screen.findAllByText(/You do not have permission to start the run/)).not.toHaveLength(0);
  });

  it('shows what went wrong for any other failure to start', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['completed']);
    runService.start.mockRejectedValue(new BackendError('request-failed', 'Starting the run failed (500): boom'));
    renderConfiguration(storeWith(VALID), runService);

    await clickRun(user);

    expect(
      (await screen.findAllByText(/The run could not be started: Starting the run failed \(500\): boom/)).length
    ).toBeGreaterThan(0);
  });

  it('reloads the Overview data when the run has finished', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const refresh = vi.spyOn(services.qualityDashboardService, 'refresh');
    const runService = runServiceWith(['running', 'completed']);
    renderOverview(
      <RunConfigFileContext.Provider value={{ store: storeWith(VALID), pollMs: 5 }}>
        <ProductShell hostInitialState={JSON.stringify({ page: 'configuration', configurationOnboardingComplete: true })} />
      </RunConfigFileContext.Provider>,
      { services: { ...services, runService } }
    );

    await clickRun(user);
    await screen.findByTestId('run-status-banner-success');

    expect(refresh).toHaveBeenCalled();
  });

  it('hides the success strip after dismiss when the run was started via the launcher', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['running', 'completed']);
    renderConfiguration(storeWith(VALID), runService);

    await clickRun(user);
    await screen.findByTestId('run-status-banner-success');

    await user.click(screen.getByTestId('run-status-banner-dismiss'));

    await waitFor(() => {
      expect(screen.queryByTestId('run-status-banner-success')).not.toBeInTheDocument();
    });
    expect(screen.queryByTestId('run-status-banner')).not.toBeInTheDocument();
  });
});

describe('Upload custom configuration file section', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('asks for a file when none was uploaded', async () => {
    renderConfiguration(createRunConfigFileStore(null));

    expect(await screen.findByTestId('run-config-upload-dropzone')).toBeInTheDocument();
    expect(screen.getByText('Upload custom configuration file')).toBeInTheDocument();
    expect(screen.getByTestId('run-config-editor-card')).toBeInTheDocument();
  });

  it('offers a sample YAML download above the upload tabs', async () => {
    const download = vi.spyOn(runConfigFileExample, 'downloadRunConfigExampleFile').mockImplementation(() => undefined);
    const user = userEvent.setup();
    renderConfiguration(createRunConfigFileStore(null));

    const strip = await screen.findByTestId('run-config-example-card');
    expect(strip).toHaveTextContent('Need help getting started?');
    expect(strip).toHaveTextContent('Download an example YAML configuration');
    await user.click(within(strip).getByRole('button', { name: 'Download example YAML' }));
    expect(download).toHaveBeenCalledTimes(1);
    download.mockRestore();
  });

  it('hides the example help banner when a configuration file is present', async () => {
    renderConfiguration(storeWith(VALID));

    await screen.findByTestId('run-config-uploaded-file');
    expect(screen.queryByTestId('run-config-example-card')).not.toBeInTheDocument();
  });

  it('shows the example help banner again after the file is removed', async () => {
    const user = userEvent.setup();
    renderConfiguration(storeWith(VALID));

    await screen.findByTestId('run-config-uploaded-file');
    expect(screen.queryByTestId('run-config-example-card')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Remove file' }));

    expect(await screen.findByTestId('run-config-example-card')).toBeInTheDocument();
  });

  it('shows Run custom configuration file after a valid upload and progress while the run is in progress', async () => {
    const user = userEvent.setup();
    const runService = runServiceWith(['running']);
    renderConfiguration(createRunConfigFileStore(null), runService);

    await upload('config.yaml', VALID);
    await dismissReviewDialog(user);

    expect(await screen.findByRole('button', { name: 'View configuration' })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId('run-button-everything')).toBeEnabled());
    await user.click(screen.getByTestId('run-button-everything'));
    await user.click(await screen.findByTestId('confirm-run-start-everything'));

    // the rerun button below shows the run in progress too, so every "Running" button is disabled
    for (const button of await screen.findAllByRole('button', { name: 'Running' })) {
      expect(button).toBeDisabled();
    }
    await screen.findByTestId('run-config-progress');
  });

  it('marks the default dimensions no completed run has measured yet', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getRunStatus').mockResolvedValue({
      lastSuccessAt: '2026-11-04T06:00:00.000Z',
      inProgress: false,
      scope: { kind: 'all' },
      dimensionRuns: { files: '2026-11-04T06:00:00.000Z' },
    });
    renderConfiguration(storeWith(VALID), runServiceWith(['completed']), services);
    await dismissReviewDialog(user);

    expect(await screen.findByTestId('rerun-dimension-not-measured-asset-hierarchy')).toHaveTextContent('Not measured yet');
    expect(screen.queryByTestId('rerun-dimension-not-measured-files')).not.toBeInTheDocument();
  });

  it('disables Run custom configuration file when the draft matches the latest completed run', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getLatestRunConfiguration').mockResolvedValue(
      LATEST_RUN_MATCHING_VALID
    );
    renderConfiguration(storeWith(VALID), runServiceWith(['completed']), services);
    await dismissReviewDialog(user);

    expect(await screen.findByTestId('run-button-everything')).toBeDisabled();
    expect(
      screen.getByText('This file matches the latest completed run. Change the configuration to run it again.')
    ).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'View configuration' })).toBeInTheDocument();
  });

  it('enables Run custom configuration file again after the YAML changes', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getLatestRunConfiguration').mockResolvedValue(
      LATEST_RUN_MATCHING_VALID
    );
    const store = storeWith(VALID);
    renderConfiguration(store, runServiceWith(['completed']), services);
    await dismissReviewDialog(user);

    store.save({
      fileName: 'config.yaml',
      text: VALID.replace('CogniteAsset.type', 'CogniteAsset.assetClass'),
    });

    await waitFor(() =>
      expect(screen.getByRole('button', { name: RUN_CUSTOM_CONFIG_LABEL })).toBeEnabled()
    );
  });

  it('shows View configuration after the configuration file run completes successfully', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getLatestRunConfiguration').mockResolvedValue(null);
    const runService = runServiceWith(['running', 'completed']);
    renderConfiguration(storeWith(VALID), runService, services);

    await dismissReviewDialog(user);
    await waitFor(() => expect(screen.getByTestId('run-button-everything')).toBeEnabled());
    await user.click(screen.getByTestId('run-button-everything'));
    await user.click(await screen.findByTestId('confirm-run-start-everything'));

    expect(await screen.findByTestId('run-status-banner-success')).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: 'View configuration' })).toBeInTheDocument();
  });

  it('reads an uploaded file and lists what the run will measure, grouped by model and view', async () => {
    const store = createRunConfigFileStore(null);
    renderConfiguration(store);

    await upload('config.yaml', VALID);

    expect(await screen.findByText('config.yaml')).toBeInTheDocument();
    expect(screen.getByText('The configuration is valid: 2 data models, 3 links.')).toBeInTheDocument();
    const model = screen.getByTestId('run-config-model-cdf_cdm:CogniteCore:v1');
    expect(within(model).getByText('CogniteAsset')).toBeInTheDocument();
    expect(within(model).getByText('type')).toBeInTheDocument();
    expect(within(model).getByText('equipmentType')).toBeInTheDocument();
    expect(within(screen.getByTestId('run-config-model-my_space:MyModel:v2')).getByText('owner')).toBeInTheDocument();
    expect(store.getSnapshot()?.fileName).toBe('config.yaml');
  });

  it('lists every problem of a file that is wrong and keeps it from being used', async () => {
    renderConfiguration(createRunConfigFileStore(null));

    await upload('bad.yaml', '- model: bad\n  links: [nope]');

    expect(await screen.findByText('The configuration has problems. Fix them and try again.')).toBeInTheDocument();
    expect(screen.getByText('The model bad must be written space:externalId:version.')).toBeInTheDocument();
    expect(screen.getByText('The link nope must be written View.property.')).toBeInTheDocument();
    expect(screen.queryByTestId('run-config-preview')).not.toBeInTheDocument();
  });

  it('keeps a file from an earlier visit without opening the review dialog until asked', async () => {
    const store = createRunConfigFileStore(null);
    store.save({ fileName: 'kept.json', text: JSON.stringify([{ model: 'a:B:v1', links: ['V.p'] }]) });

    renderConfiguration(store);

    expect(await screen.findByText('kept.json')).toBeInTheDocument();
    expect(screen.queryByTestId('run-config-review-dialog')).not.toBeInTheDocument();
  });

  it('removes the file', async () => {
    const user = userEvent.setup();
    const store = createRunConfigFileStore(null);
    renderConfiguration(store);
    await upload('config.yaml', VALID);
    await dismissReviewDialog(user);

    await user.click(await screen.findByRole('button', { name: 'Remove file' }));

    expect(store.getSnapshot()).toBeNull();
    await user.click(screen.getByRole('tab', { name: 'Upload file' }));
    expect(await screen.findByTestId('run-config-upload-dropzone')).toBeInTheDocument();
  });

  it('replaces the file with the next upload', async () => {
    const user = userEvent.setup();
    const store = createRunConfigFileStore(null);
    renderConfiguration(store);
    await upload('first.yaml', VALID);
    await screen.findByText('first.yaml');

    await dismissReviewDialog(user);
    await user.click(screen.getByRole('tab', { name: 'Upload file' }));
    await upload('second.yaml', '- model: a:B:v1\n  links: [V.p]');

    await waitFor(() => expect(screen.getByText('second.yaml')).toBeInTheDocument());
    expect(screen.getByText('The configuration is valid: 1 data models, 1 links.')).toBeInTheDocument();
  });

  it('accepts pasted YAML in the editor and enables Run when it differs from the latest run', async () => {
    const user = userEvent.setup();
    const store = createRunConfigFileStore(null);
    renderConfiguration(store);

    await user.click(await screen.findByRole('tab', { name: 'Paste YAML' }));
    const editor = screen.getByTestId('run-config-editor');
    await user.click(editor);
    await user.paste(VALID);

    expect(screen.queryByTestId('run-config-review-dialog')).not.toBeInTheDocument();
    expect(await screen.findByRole('button', { name: RUN_CUSTOM_CONFIG_LABEL })).toBeEnabled();
    expect(store.getSnapshot()?.fileName).toBe('config.yaml');
  });

  it('does not open the review dialog while typing in Paste YAML', async () => {
    const user = userEvent.setup();
    renderConfiguration(createRunConfigFileStore(null));

    await user.click(await screen.findByRole('tab', { name: 'Paste YAML' }));
    const editor = screen.getByTestId('run-config-editor');
    await user.click(editor);
    await user.type(editor, '- model: a:B:v1\n  links:\n    - V.p');

    expect(screen.queryByTestId('run-config-review-dialog')).not.toBeInTheDocument();
  });
});

function renderConfiguration(
  store: RunConfigFileStore,
  runService?: RunService | null,
  services?: ReturnType<typeof createMockServices>
) {
  const baseServices = services ?? createMockServices({ latency: 0 });
  return renderOverview(
    <RunConfigFileContext.Provider value={{ store, pollMs: 5 }}>
      <ProductShell hostInitialState={JSON.stringify({ page: 'configuration', configurationOnboardingComplete: true })} />
    </RunConfigFileContext.Provider>,
    { services: runService === undefined ? baseServices : { ...baseServices, runService } }
  );
}

function runServiceWith(statuses: ('running' | 'completed' | 'failed')[]): RunService & {
  start: ReturnType<typeof vi.fn>;
} {
  let polled = 0;
  return {
    start: vi.fn(async () => ({ executionId: 'exec-1' })),
    getStatus: vi.fn(async () => statuses[Math.min(polled++, statuses.length - 1)] ?? 'failed'),
    findRunning: vi.fn(async () => false),
  };
}

function storeWith(text: string): RunConfigFileStore {
  const store = createRunConfigFileStore(null);
  store.save({ fileName: 'config.yaml', text });
  return store;
}

async function upload(name: string, text: string): Promise<void> {
  const user = userEvent.setup();
  const input = await screen.findByTestId('run-config-file-input');
  await user.upload(input, new File([text], name, { type: 'text/plain' }));
}

async function dismissReviewDialog(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  const dialog = screen.queryByTestId('run-config-review-dialog');
  if (dialog === null) {
    return;
  }
  await user.keyboard('{Escape}');
}

async function clickRun(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  await dismissReviewDialog(user);
  await user.click(await screen.findByRole('checkbox', { name: 'Files' }));
  const button = await screen.findByRole('button', { name: 'Run selected (1)' });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
  // a run is only started after the person has read what it does and confirmed
  await user.click(await screen.findByTestId('confirm-run-start-selection'));
}
