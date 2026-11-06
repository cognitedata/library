import { CogniteClient, type ClientOptions } from '@cognite/sdk';
import { screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import App from './App';
import type { AppDeps } from './app/defaultAppDeps';
import { makeMockHostApi } from './test/mockHostApi';
import { renderApp } from './test/renderApp';

function makeDeps(connectToHostApp: AppDeps['connectToHostApp']): AppDeps {
  return {
    connectToHostApp,
    createClient: vi.fn((config: ClientOptions) => new CogniteClient(config)),
  };
}

describe(App.name, () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders loading state while the host connection is pending', () => {
    const connectToHostApp = vi.fn(
      () => new Promise<{ api: ReturnType<typeof makeMockHostApi> }>(() => undefined)
    );

    renderApp(<App deps={makeDeps(connectToHostApp)} />);

    expect(screen.getByRole('status')).toHaveTextContent('Loading application...');
    expect(screen.queryByRole('banner')).not.toBeInTheDocument();
  });

  it('renders the product shell after a successful host connection', async () => {
    const connectToHostApp = vi.fn(() =>
      Promise.resolve({ api: makeMockHostApi() })
    );

    renderApp(<App deps={makeDeps(connectToHostApp)} />);

    await waitFor(() => {
      expect(productNameInTopbar()).toBeInTheDocument();
    });

    expect(screen.queryByText('Welcome to Flows custom apps')).not.toBeInTheDocument();
    expect(screen.queryByText('Get started')).not.toBeInTheDocument();
  });

  it('renders the product shell when the host connection is rejected', async () => {
    const connectToHostApp = vi.fn(() =>
      Promise.reject(new Error('No Fusion host'))
    );

    renderApp(<App deps={makeDeps(connectToHostApp)} />);

    await waitFor(() => {
      expect(productNameInTopbar()).toBeInTheDocument();
    });

    expect(screen.queryByText('Welcome to Flows custom apps')).not.toBeInTheDocument();
  });

  it('calls connectToHostApp only once despite host resolution and CogniteSdkProvider', async () => {
    const connectToHostApp = vi.fn(() =>
      Promise.resolve({ api: makeMockHostApi() })
    );

    renderApp(<App deps={makeDeps(connectToHostApp)} />);

    await waitFor(() => {
      expect(productNameInTopbar()).toBeInTheDocument();
    });

    expect(connectToHostApp).toHaveBeenCalledTimes(1);
    expect(connectToHostApp).toHaveBeenCalledWith({
      applicationName: 'context-quality-dashboard',
    });
  });
});

function productNameInTopbar() {
  return screen.getByRole('heading', {
    level: 1,
    name: 'Contextualization Quality',
  });
}
