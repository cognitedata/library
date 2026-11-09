import { Loader } from '@cognite/aura/components/loader';

import type { BackendConfig } from '../services/cdf/streamConfig';

import { bootLoadingDescription, LOADING_HEADER, type BootLoadingStep } from './loadingSteps';

type LoadingFallbackProps = {
  step?: BootLoadingStep;
  config?: BackendConfig;
};

export function LoadingFallback({ step, config }: LoadingFallbackProps) {
  const description =
    step !== undefined ? bootLoadingDescription(step, config) : undefined;

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4 text-foreground">
      <Loader variant="page" header={LOADING_HEADER} description={description} />
    </div>
  );
}

export { LOADING_HEADER };
