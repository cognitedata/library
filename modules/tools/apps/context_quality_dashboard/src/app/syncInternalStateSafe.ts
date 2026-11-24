import type { HostAppAPI } from '@cognite/app-sdk';

/**
 * Pushes view state to the host so reloads and shared links restore it.
 * Hosts that lack the method, or fail the call, only lose URL persistence; the app keeps working.
 */
export async function syncInternalStateSafe(
  api: HostAppAPI | null,
  state: string
): Promise<boolean> {
  if (api === null || typeof api.syncInternalState !== 'function') {
    return false;
  }
  try {
    return await api.syncInternalState(state);
  } catch {
    return false;
  }
}
