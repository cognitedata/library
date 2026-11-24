import type { ConnectToHostAppResult } from '@cognite/app-sdk';

type ConnectToHostAppOptions = {
  applicationName?: string;
};

export type ConnectToHostAppFn = (
  meta?: ConnectToHostAppOptions
) => Promise<ConnectToHostAppResult>;

/**
 * Wraps connectToHostApp so the underlying handshake runs at most once per app load.
 * Shared by the host-resolution effect and CogniteSdkProvider deps.
 */
export function createConnectOnce(connectFn: ConnectToHostAppFn): ConnectToHostAppFn {
  let cached: ConnectToHostAppResult | undefined;
  let inflight: Promise<ConnectToHostAppResult> | undefined;

  return (meta?: ConnectToHostAppOptions) => {
    if (cached !== undefined) {
      return Promise.resolve(cached);
    }
    if (inflight === undefined) {
      inflight = connectFn(meta)
        .then((result) => {
          cached = result;
          return result;
        })
        .catch((error: unknown) => {
          inflight = undefined;
          return Promise.reject(error);
        });
    }
    return inflight;
  };
}
