import { describe, expect, it, vi } from 'vitest';

import { makeMockHostApi } from '../test/mockHostApi';

import { createConnectOnce } from './createConnectOnce';

describe(createConnectOnce.name, () => {
  it('invokes the underlying connect function only once', async () => {
    const connectToHostApp = vi.fn(() =>
      Promise.resolve({ api: makeMockHostApi() })
    );
    const connectOnce = createConnectOnce(connectToHostApp);

    await connectOnce({ applicationName: 'context-quality-dashboard' });
    await connectOnce({ applicationName: 'context-quality-dashboard' });

    expect(connectToHostApp).toHaveBeenCalledTimes(1);
  });

  it('rejects all callers when the underlying connect function fails', async () => {
    const connectToHostApp = vi.fn(() =>
      Promise.reject(new Error('connection failed'))
    );
    const connectOnce = createConnectOnce(connectToHostApp);

    await expect(connectOnce()).rejects.toThrow('connection failed');
    await expect(connectOnce()).rejects.toThrow('connection failed');

    expect(connectToHostApp).toHaveBeenCalledTimes(2);
  });
});
