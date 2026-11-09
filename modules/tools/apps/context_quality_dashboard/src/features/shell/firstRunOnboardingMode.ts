type EnvLike = Readonly<Record<string, unknown>>;

export type { EnvLike };

function readFlag(env: EnvLike, key: string): boolean {
  const value = env[key];
  return value === true || value === 'true';
}

/** Local preview of first-visit onboarding without an empty CDF stream. Dev builds only. */
export function isFirstRunOnboardingPreview(env: EnvLike = import.meta.env): boolean {
  return env.DEV === true && readFlag(env, 'VITE_DEV_FORCE_FIRST_RUN_ONBOARDING');
}

export function shouldShowFirstRunOnboarding(hasNoRunYet: boolean, env: EnvLike = import.meta.env): boolean {
  return hasNoRunYet || isFirstRunOnboardingPreview(env);
}

/** Starting the built-in checks on the first visit is off unless the build asks for it; otherwise the user starts them. */
export function isBuiltInAutoStartEnabled(env: EnvLike = import.meta.env): boolean {
  return readFlag(env, 'VITE_CQ_AUTO_START_FIRST_RUN');
}
