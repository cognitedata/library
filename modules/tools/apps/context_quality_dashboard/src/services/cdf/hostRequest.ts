import type { HostAppAPI } from '@cognite/app-sdk';

import { BackendError } from './backendErrors';

export type HostConnection = {
  baseUrl: string;
  token: string;
  project: string;
};

/** Project, cluster URL and token all come from the Fusion host; nothing here is configured per deployment. */
export async function resolveHost(getHostApi: () => HostAppAPI | null): Promise<HostConnection> {
  const api = getHostApi();
  if (api === null) {
    throw new BackendError(
      'host-unavailable',
      'The Fusion host is not connected, so the Context Quality backend cannot be read. Open the dashboard from Fusion. For local development without Fusion, set VITE_QUALITY_BACKEND=mock.'
    );
  }
  const [baseUrl, token, project] = await Promise.all([
    api.getBaseUrl(),
    api.getAccessToken(),
    api.getProject(),
  ]);
  return { baseUrl, token, project };
}
