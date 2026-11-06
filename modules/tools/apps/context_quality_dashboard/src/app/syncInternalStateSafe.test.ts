import type { HostAppAPI } from '@cognite/app-sdk';
import { describe, expect, it, vi } from 'vitest';

import { syncInternalStateSafe } from './syncInternalStateSafe';

describe(syncInternalStateSafe.name, () => {
  it('does nothing without a host', async () => {
    await expect(syncInternalStateSafe(null, '{}')).resolves.toBe(false);
  });

  it('forwards the state to the host', async () => {
    const syncInternalState = vi.fn(async () => true);

    const result = await syncInternalStateSafe(hostWith({ syncInternalState }), '{"page":"overview"}');

    expect(result).toBe(true);
    expect(syncInternalState).toHaveBeenCalledWith('{"page":"overview"}');
  });

  it('returns false when the host does not provide syncInternalState', async () => {
    await expect(syncInternalStateSafe(hostWith({}), '{}')).resolves.toBe(false);
  });

  it('returns false instead of throwing when the host call fails', async () => {
    const syncInternalState = vi.fn(async () => {
      throw new Error('host unavailable');
    });

    await expect(syncInternalStateSafe(hostWith({ syncInternalState }), '{}')).resolves.toBe(false);
  });
});

function hostWith(methods: Partial<HostAppAPI>): HostAppAPI {
  return methods as HostAppAPI;
}
