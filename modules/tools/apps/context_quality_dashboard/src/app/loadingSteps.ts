import type { BackendConfig } from '../services/cdf/streamConfig';

/**
 * Boot-time loading only (before ProductShell). After the shell mounts, run status and page data load
 * inside the app with their own skeletons — not covered here.
 *
 * Order:
 * 1. BackendBootstrap — resolve env, build service layer (mock: async chunk; cdf: sync wiring, no CDF API yet)
 * 2. App — connectToHostApp (Fusion host API, URL state)
 * 3. CogniteSdkProvider — only when host connected; Cognite client + project auth
 */
export const LOADING_HEADER = 'Loading application...';

export type BootLoadingStep =
  | 'prepare-services'
  | 'connect-fusion'
  | 'authenticate-cdf';

const STEP_DESCRIPTION: Readonly<Record<BootLoadingStep, string>> = {
  'prepare-services':
    'Preparing dashboard services from your deployment settings. No quality data is read yet.',
  'connect-fusion': 'Connecting to Cognite Fusion and restoring your view from the link.',
  'authenticate-cdf': 'Signing in to your Cognite Data Fusion project.',
};

const MOCK_PREPARE_DESCRIPTION =
  'Loading built-in sample data services. This is not data from your project.';

export function bootLoadingDescription(step: BootLoadingStep, config?: BackendConfig): string {
  if (step === 'prepare-services' && config?.backend === 'mock') {
    return MOCK_PREPARE_DESCRIPTION;
  }
  return STEP_DESCRIPTION[step];
}
