import { screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { DEFAULT_HOST_SYNCED_STATE } from '../../app/state/defaultState';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';

import { ConfigurationPage } from './ConfigurationPage';

function renderConfiguration(
  hostState: Record<string, unknown> = { page: 'configuration', configurationOnboardingComplete: true }
) {
  return renderOverview(<ProductShell hostInitialState={JSON.stringify(hostState)} />, {
    services: createMockServices({ latency: 0 }),
  });
}

describe(ConfigurationPage.name, () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders the run configuration file section only', async () => {
    renderConfiguration();

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Upload custom configuration file' })).toBeInTheDocument();
    });

    expect(screen.getByTestId('run-config-example-card')).toBeInTheDocument();
    expect(screen.queryByTestId('feature-flags-list')).not.toBeInTheDocument();
    expect(screen.queryByTestId('owner-mappings-table')).not.toBeInTheDocument();
    expect(screen.queryByTestId('user-exclusions-list')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Custom Quality Rules')).not.toBeInTheDocument();
  });

  it('renders when mounted directly', () => {
    renderOverview(<ConfigurationPage />, {
      services: createMockServices({ latency: 0 }),
      hostInitialState: JSON.stringify({ ...DEFAULT_HOST_SYNCED_STATE, page: 'configuration' }),
    });

    expect(screen.getByRole('heading', { name: 'Upload custom configuration file' })).toBeInTheDocument();
  });
});
