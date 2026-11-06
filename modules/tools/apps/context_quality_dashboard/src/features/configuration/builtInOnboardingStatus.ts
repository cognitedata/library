import type { RunPhase } from './useRunConfigFileViewModel';

export type BuiltInOnboardingStatus = 'unavailable' | 'pending' | 'running' | 'completed' | 'failed';

export type DeriveBuiltInOnboardingStatusInput = {
  canRun: boolean;
  hasNoRunYet: boolean;
  configPhase: RunPhase;
  configStartError: { key: string; message: string } | null;
  backendRunInProgress: boolean;
  builtInAutoRunAttempted: boolean;
};

/** Status for the built-in checks card on first-run onboarding. */
export function deriveBuiltInOnboardingStatus(
  input: DeriveBuiltInOnboardingStatusInput
): BuiltInOnboardingStatus {
  if (!input.canRun) {
    return 'unavailable';
  }

  if (!input.hasNoRunYet) {
    return 'completed';
  }

  const configActive = input.configPhase === 'starting' || input.configPhase === 'running';
  if (configActive || input.backendRunInProgress) {
    return 'running';
  }

  if (input.configPhase === 'failed') {
    return 'failed';
  }

  if (input.configPhase === 'not-started' && input.configStartError !== null && input.builtInAutoRunAttempted) {
    return 'failed';
  }

  if (input.configPhase === 'completed') {
    return 'completed';
  }

  return 'pending';
}

export function shouldAutoStartBuiltInRun(input: DeriveBuiltInOnboardingStatusInput): boolean {
  if (!input.canRun || !input.hasNoRunYet || input.builtInAutoRunAttempted) {
    return false;
  }
  if (input.backendRunInProgress) {
    return false;
  }
  const configActive = input.configPhase === 'starting' || input.configPhase === 'running';
  if (configActive) {
    return false;
  }
  return input.configPhase === 'idle';
}
