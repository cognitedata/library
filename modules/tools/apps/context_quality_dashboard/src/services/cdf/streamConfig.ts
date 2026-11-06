/**
 * How the dashboard finds the backend it reads from.
 *
 * The backend (stream, schema space, containers) is deployed and owned outside this repository.
 * The dashboard only consumes it, so it is told where it is through build-time `VITE_*` variables:
 * the installed Fusion app SDK (`@cognite/app-sdk`) offers the project, base URL and token at runtime
 * but no way to read per-deployment settings. There are deliberately no defaults for the stream or
 * the schema space: a default would silently point every deployment at one project's resources.
 */
export type StreamConfig = {
  streamId: string;
  schemaSpace: string;
  relationshipContainer: string;
  /**
   * Upper bound, in days, on how much history to read. `null` means "as much as the deployed stream allows".
   * The stream's own filter limit always applies on top (see HttpRecordsClient).
   */
  windowDays: number | null;
};

/** The deployed workflow a run is started with. Without it the dashboard only reads runs and offers no Run button. */
export type WorkflowConfig = { externalId: string; version: string };

export type BackendConfig =
  | { backend: 'mock' }
  | { backend: 'cdf'; stream: StreamConfig; workflow: WorkflowConfig | null };

export type ConfigIssue = {
  variable: string;
  problem: 'missing' | 'invalid';
  message: string;
};

export type BackendConfigResult =
  | { ok: true; config: BackendConfig }
  | { ok: false; issues: readonly ConfigIssue[] };

type EnvLike = Readonly<Record<string, unknown>>;

/** The run-log container name is fixed by the backend's data model, not a deployment choice. */
export const DEFAULT_RELATIONSHIP_CONTAINER = 'ContextQualityRelationship';

/** The window used when the stream's limits cannot be read: the smallest filter interval a stream template allows. */
export const FALLBACK_WINDOW_DAYS = 7;

/** The longest trend window the dashboard offers, so reading further back is never useful. */
export const MAX_WINDOW_DAYS = 90;

const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

function readValue(env: EnvLike, key: string): string | null {
  const value = env[key];
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function readIdentifier(env: EnvLike, key: string, issues: ConfigIssue[]): string | null {
  const value = readValue(env, key);
  if (value === null) {
    issues.push({ variable: key, problem: 'missing', message: `${key} is not set.` });
    return null;
  }
  if (!IDENTIFIER.test(value)) {
    issues.push({
      variable: key,
      problem: 'invalid',
      message: `${key} must contain only letters, digits, "_" and "-" (got "${value}").`,
    });
    return null;
  }
  return value;
}

function readWindowDays(env: EnvLike, issues: ConfigIssue[]): number | null {
  const raw = readValue(env, 'VITE_CQ_WINDOW_DAYS');
  if (raw === null) {
    return null;
  }
  const days = Number(raw);
  if (!Number.isInteger(days) || days < 1 || days > 365) {
    issues.push({
      variable: 'VITE_CQ_WINDOW_DAYS',
      problem: 'invalid',
      message: `VITE_CQ_WINDOW_DAYS must be a whole number of days between 1 and 365 (got "${raw}").`,
    });
    return null;
  }
  return days;
}

/** Both variables or neither: a workflow id without its version (or the reverse) is a mistake, not "no workflow". */
function readWorkflow(env: EnvLike, issues: ConfigIssue[]): WorkflowConfig | null {
  const hasId = readValue(env, 'VITE_CQ_WORKFLOW_ID') !== null;
  const hasVersion = readValue(env, 'VITE_CQ_WORKFLOW_VERSION') !== null;
  if (!hasId && !hasVersion) {
    return null;
  }
  const externalId = readIdentifier(env, 'VITE_CQ_WORKFLOW_ID', issues);
  const version = readIdentifier(env, 'VITE_CQ_WORKFLOW_VERSION', issues);
  return externalId === null || version === null ? null : { externalId, version };
}

/**
 * Reads the deployment configuration. `VITE_QUALITY_BACKEND` must be set explicitly:
 *  - `cdf`  reads the deployed metrics stream (needs the stream and schema-space variables),
 *  - `mock` uses the built-in demo dataset and is meant for development and demos only.
 * Anything else, including nothing, is a configuration error. There is no implicit mode.
 */
export function resolveBackendConfig(env: EnvLike = import.meta.env): BackendConfigResult {
  const selected = readValue(env, 'VITE_QUALITY_BACKEND');
  if (selected === null) {
    return {
      ok: false,
      issues: [
        {
          variable: 'VITE_QUALITY_BACKEND',
          problem: 'missing',
          message: 'VITE_QUALITY_BACKEND is not set. Use "cdf" to read a deployed backend, or "mock" for the demo dataset.',
        },
      ],
    };
  }
  if (selected === 'mock') {
    return { ok: true, config: { backend: 'mock' } };
  }
  if (selected !== 'cdf') {
    return {
      ok: false,
      issues: [
        {
          variable: 'VITE_QUALITY_BACKEND',
          problem: 'invalid',
          message: `VITE_QUALITY_BACKEND must be "cdf" or "mock" (got "${selected}").`,
        },
      ],
    };
  }

  const issues: ConfigIssue[] = [];
  const streamId = readIdentifier(env, 'VITE_CQ_STREAM_ID', issues);
  const schemaSpace = readIdentifier(env, 'VITE_CQ_SCHEMA_SPACE', issues);
  const containerValue = readValue(env, 'VITE_CQ_RELATIONSHIP_CONTAINER');
  const relationshipContainer =
    containerValue === null
      ? DEFAULT_RELATIONSHIP_CONTAINER
      : readIdentifier(env, 'VITE_CQ_RELATIONSHIP_CONTAINER', issues);
  const windowDays = readWindowDays(env, issues);
  const workflow = readWorkflow(env, issues);

  if (issues.length > 0 || streamId === null || schemaSpace === null || relationshipContainer === null) {
    return { ok: false, issues };
  }
  return {
    ok: true,
    config: { backend: 'cdf', stream: { streamId, schemaSpace, relationshipContainer, windowDays }, workflow },
  };
}
