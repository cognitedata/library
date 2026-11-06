import { connectToHostApp } from '@cognite/app-sdk';
import { CogniteClient, type ClientOptions } from '@cognite/sdk';

import type { ConnectToHostAppFn } from './createConnectOnce';

export type AppDeps = {
  connectToHostApp: ConnectToHostAppFn;
  createClient: (config: ClientOptions) => CogniteClient;
};

export const defaultAppDeps: AppDeps = {
  connectToHostApp,
  createClient: (config: ClientOptions) => new CogniteClient(config),
};
