import type { HostSyncedState } from '../../app/state/types';
import {
  isFirstRunOnboardingPreview,
  type EnvLike,
} from '../shell/firstRunOnboardingMode';

export function shouldShowConfigurationOnboarding(
  state: HostSyncedState,
  hasNoRunYet: boolean,
  env: EnvLike = import.meta.env
): boolean {
  if (state.configurationOnboardingComplete === true) {
    return false;
  }
  if (isFirstRunOnboardingPreview(env)) {
    return true;
  }
  if (!hasNoRunYet) {
    return false;
  }
  return true;
}

export { isFirstRunOnboardingPreview };
