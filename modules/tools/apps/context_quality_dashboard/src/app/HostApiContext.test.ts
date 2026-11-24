import { describe, expect, it } from 'vitest';

import { makeMockHostApi } from '../test/mockHostApi';

import { syncInternalStateSafe } from './syncInternalStateSafe';

describe(syncInternalStateSafe.name, () => {
  it('no-ops when the host API is unavailable', async () => {
    const result = await syncInternalStateSafe(null, '{"page":"overview"}');

    expect(result).toBe(false);
  });

  it('delegates to the host API when available', async () => {
    const api = makeMockHostApi();

    const result = await syncInternalStateSafe(api, '{"page":"overview"}');

    expect(result).toBe(true);
    expect(api.syncInternalState).toHaveBeenCalledWith('{"page":"overview"}');
  });
});
