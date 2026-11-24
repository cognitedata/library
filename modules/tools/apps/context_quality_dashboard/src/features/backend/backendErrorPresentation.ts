import { isBackendError, type BackendErrorKind } from '../../services/cdf/backendErrors';

export type BackendErrorPresentation = {
  titleKey: string;
  hintKey: string;
  /** The ordered next steps, as translation keys. */
  stepKeys: readonly string[];
  /** The one thing the app itself can do about it, if anything. Trying again is `canRetry`, not an action. */
  primaryAction: 'configuration' | null;
  /** `info` for "nothing to show yet", which is a state to move on from; `error` for a failure someone has to fix. */
  tone: 'info' | 'error';
  /** Technical text for the people who run the backend; never translated. */
  details: string;
  /** Whether trying again can help without anyone changing configuration or deployments. */
  canRetry: boolean;
};

type PresentOptions = {
  /** Whether this build can start a run, which decides what the "no run yet" steps tell the user to do. */
  canStartRun?: boolean;
};

const STEP_COUNTS: Readonly<Record<string, number>> = {
  'host-unavailable': 3,
  unauthenticated: 3,
  'permission-denied': 3,
  'stream-missing': 3,
  'schema-missing': 3,
  'window-too-long': 2,
  'unsupported-operation': 2,
  'request-failed': 3,
  network: 3,
  unknown: 2,
};

const KNOWN_KINDS: readonly BackendErrorKind[] = [
  'host-unavailable',
  'unauthenticated',
  'permission-denied',
  'stream-missing',
  'schema-missing',
  'window-too-long',
  'no-completed-run',
  'unsupported-operation',
  'request-failed',
  'network',
];

function isKnownKind(kind: string): kind is BackendErrorKind {
  return KNOWN_KINDS.some((known) => known === kind);
}

function stepKeysOf(prefix: string, count: number): string[] {
  return Array.from({ length: count }, (_, index) => `${prefix}.steps.${index + 1}`);
}

/** Which translation keys explain a failure. Anything that is not a typed backend error gets the generic text. */
export function presentBackendError(error: unknown, options: PresentOptions = {}): BackendErrorPresentation {
  if (!isBackendError(error) || !isKnownKind(error.kind)) {
    return {
      titleKey: 'backend.error.unknown.title',
      hintKey: 'backend.error.unknown.hint',
      stepKeys: stepKeysOf('backend.error.unknown', STEP_COUNTS.unknown ?? 0),
      primaryAction: null,
      tone: 'error',
      details: error instanceof Error ? error.message : String(error),
      canRetry: true,
    };
  }
  const prefix = `backend.error.${error.kind}`;
  const canRetry = error.kind !== 'window-too-long' && error.kind !== 'schema-missing';
  if (error.kind === 'no-completed-run') {
    return presentNoRun(error.detail, error.message, canRetry, options);
  }
  return {
    titleKey: `${prefix}.title`,
    hintKey: `${prefix}.hint`,
    stepKeys: stepKeysOf(prefix, STEP_COUNTS[error.kind] ?? 0),
    primaryAction: null,
    tone: 'error',
    details: error.message,
    canRetry,
  };
}

/** Having no completed run is where a new user starts, not a crash: it gets neutral chrome and a way forward. */
function presentNoRun(
  detail: string | undefined,
  details: string,
  canRetry: boolean,
  options: PresentOptions
): BackendErrorPresentation {
  const prefix = 'backend.error.no-completed-run';
  if (detail === 'empty') {
    const steps = stepKeysOf(`${prefix}.empty`, 4);
    return {
      titleKey: `${prefix}.title`,
      hintKey: `${prefix}.empty`,
      // without a way to start a run, the third step says who has to
      stepKeys: options.canStartRun === false ? steps.map((key) => (key.endsWith('.3') ? `${key}.noRun` : key)) : steps,
      primaryAction: 'configuration',
      tone: 'info',
      details,
      canRetry,
    };
  }
  if (detail === 'incomplete') {
    return {
      titleKey: `${prefix}.incomplete.title`,
      hintKey: `${prefix}.incomplete`,
      stepKeys: stepKeysOf(`${prefix}.incomplete`, 3),
      primaryAction: 'configuration',
      tone: 'info',
      details,
      canRetry,
    };
  }
  return {
    titleKey: `${prefix}.title`,
    hintKey: `${prefix}.hint`,
    stepKeys: stepKeysOf(prefix, 3),
    primaryAction: 'configuration',
    tone: 'info',
    details,
    canRetry,
  };
}
