/**
 * Failures the dashboard can tell apart when it reads the backend. The backend is deployed and owned
 * elsewhere: none of these are fixed by the dashboard creating anything, and none fall back to demo data.
 */
export type BackendErrorKind =
  /** The app is not connected to a Fusion host, so there is no project, URL or token. */
  | 'host-unavailable'
  /** HTTP 401: the session is missing or expired. */
  | 'unauthenticated'
  /** HTTP 403: the signed-in user lacks a capability the dashboard needs. */
  | 'permission-denied'
  /** The configured stream does not exist in the project. */
  | 'stream-missing'
  /** The configured schema space, container or a contract property does not exist. */
  | 'schema-missing'
  /** The configured history window is longer than the stream allows. */
  | 'window-too-long'
  /** The stream has no completed run (empty stream, runs unfinished, or runs expired). */
  | 'no-completed-run'
  /** The action has no backend implementation. */
  | 'unsupported-operation'
  /** Any other API failure (for example 5xx, or rate limiting that did not clear). */
  | 'request-failed'
  /** The request never reached the API. */
  | 'network';

export type BackendErrorOptions = {
  /** What the failure is about: a stream id, a schema space, a capability, a version. */
  subject?: string;
  status?: number;
  /** Extra machine-readable context, for example `empty` or `incomplete` for a missing run. */
  detail?: string;
};

export class BackendError extends Error {
  readonly kind: BackendErrorKind;
  readonly subject: string | undefined;
  readonly status: number | undefined;
  readonly detail: string | undefined;

  constructor(kind: BackendErrorKind, message: string, options: BackendErrorOptions = {}) {
    super(message);
    this.name = 'BackendError';
    this.kind = kind;
    this.subject = options.subject;
    this.status = options.status;
    this.detail = options.detail;
  }
}

export function isBackendError(value: unknown): value is BackendError {
  return value instanceof BackendError;
}

/** What the API said when a request failed; the body is only used to tell "missing" from "malformed". */
export function apiMessage(body: string): string {
  try {
    const parsed: unknown = JSON.parse(body);
    if (typeof parsed === 'object' && parsed !== null && 'error' in parsed) {
      const error = parsed.error;
      if (typeof error === 'object' && error !== null && 'message' in error && typeof error.message === 'string') {
        return error.message;
      }
    }
  } catch {
    // not JSON: use the raw text
  }
  return body.trim();
}

type FailureContext = {
  streamId: string;
  schemaSpace: string;
  /** What was being done, for the message (for example "Records query"). */
  action: string;
};

/**
 * Maps a non-OK response to a typed error. The message patterns come from the live Records and
 * Streams API: a missing stream answers 400 "Stream <id> not found" on a filter call and 404 on a
 * stream lookup, and an unknown space, container or property answers 400 "Unknown space|property: ...".
 */
export function classifyFailure(status: number, body: string, context: FailureContext): BackendError {
  const message = apiMessage(body);
  const summary = `${context.action} failed (${status})${message === '' ? '' : `: ${message.slice(0, 300)}`}`;
  if (status === 401) {
    return new BackendError('unauthenticated', summary, { status });
  }
  if (status === 403) {
    return new BackendError('permission-denied', summary, { status, subject: context.streamId });
  }
  if (status === 404 || /stream .*not found/i.test(message)) {
    return new BackendError('stream-missing', summary, { status, subject: context.streamId });
  }
  if (/unknown (space|container|property|view)/i.test(message)) {
    return new BackendError('schema-missing', summary, { status, subject: context.schemaSpace });
  }
  if (/interval.*(too big|max)|max allowed interval/i.test(message)) {
    return new BackendError('window-too-long', summary, { status, detail: message });
  }
  return new BackendError('request-failed', summary, { status });
}

export function networkFailure(action: string, cause: unknown): BackendError {
  const reason = cause instanceof Error ? cause.message : String(cause);
  return new BackendError('network', `${action} could not reach the API: ${reason}`);
}
