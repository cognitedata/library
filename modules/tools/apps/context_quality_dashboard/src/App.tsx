import type { HostAppAPI } from '@cognite/app-sdk';
import { CogniteSdkProvider } from '@cognite/app-sdk/react';
import { useEffect, useMemo, useState } from 'react';

import { APPLICATION_NAME } from './app/constants';
import { createConnectOnce } from './app/createConnectOnce';
import { defaultAppDeps, type AppDeps } from './app/defaultAppDeps';
import { HostApiProvider } from './app/HostApiContext';
import { hostApiHolder } from './app/hostApiHolder';
import { LoadingFallback } from './app/LoadingFallback';
import { ProductShell } from './app/ProductShell';

type AppProps = {
  deps?: AppDeps;
};

export default function App({ deps = defaultAppDeps }: AppProps) {
  const connectOnce = useMemo(
    () => createConnectOnce(deps.connectToHostApp),
    [deps.connectToHostApp]
  );

  const providerDeps = useMemo(
    () => ({
      connectToHostApp: connectOnce,
      createClient: deps.createClient,
    }),
    [connectOnce, deps.createClient]
  );

  const [hostApi, setHostApi] = useState<HostAppAPI | null>(null);
  const [hostInitialState, setHostInitialState] = useState<string | undefined>();
  const [hostResolved, setHostResolved] = useState(false);

  useEffect(() => {
    let cancelled = false;

    connectOnce({ applicationName: APPLICATION_NAME })
      .then((result) => {
        if (!cancelled) {
          hostApiHolder.current = result.api;
          setHostApi(result.api);
          setHostInitialState(result.initialState);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setHostApi(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setHostResolved(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [connectOnce]);

  if (!hostResolved) {
    return <LoadingFallback step="connect-fusion" />;
  }

  const shell = <ProductShell hostInitialState={hostInitialState} />;

  return (
    <HostApiProvider api={hostApi}>
      {hostApi === null ? (
        shell
      ) : (
        <CogniteSdkProvider
          deps={providerDeps}
          loadingFallback={<LoadingFallback step="authenticate-cdf" />}
          errorFallback={shell}
        >
          {shell}
        </CogniteSdkProvider>
      )}
    </HostApiProvider>
  );
}

export type { AppDeps };
