import { describe, expect, it } from 'vitest';

import { apiMessage, BackendError, classifyFailure, isBackendError, networkFailure } from './backendErrors';

const context = { streamId: 'cq_stream', schemaSpace: 'cq_schema', action: 'Records query' };

function failure(status: number, message: string) {
  return classifyFailure(status, JSON.stringify({ error: { code: status, message } }), context);
}

describe(classifyFailure.name, () => {
  it('maps 401 to unauthenticated', () => {
    expect(failure(401, 'Unauthorized')).toMatchObject({ kind: 'unauthenticated', status: 401 });
  });

  it('maps 403 to permission-denied and names the stream', () => {
    expect(failure(403, 'Forbidden')).toMatchObject({ kind: 'permission-denied', subject: 'cq_stream' });
  });

  it('maps the 404 of a stream lookup to stream-missing', () => {
    expect(failure(404, 'Stream with externalId cq_stream not found')).toMatchObject({ kind: 'stream-missing' });
  });

  it('maps the 400 the Records filter returns for a missing stream to stream-missing', () => {
    expect(failure(400, 'Stream cq_stream not found')).toMatchObject({ kind: 'stream-missing', subject: 'cq_stream' });
  });

  it.each(['Unknown space: cq_schema', 'Unknown property: cq_schema.ContextQualityRelationship.nope', 'Unknown container: X'])(
    'maps "%s" to schema-missing',
    (message) => {
      expect(failure(400, message)).toMatchObject({ kind: 'schema-missing', subject: 'cq_schema' });
    }
  );

  it('maps a too-long interval to window-too-long', () => {
    expect(failure(400, "lastUpdatedTime interval='44d' is to big. Max allowed interval='7d'")).toMatchObject({
      kind: 'window-too-long',
    });
  });

  it('keeps the status and API text for anything else', () => {
    const error = failure(500, 'boom');

    expect(error).toMatchObject({ kind: 'request-failed', status: 500 });
    expect(error.message).toContain('500');
    expect(error.message).toContain('boom');
  });

  it('copes with a body that is not JSON', () => {
    const error = classifyFailure(502, 'Bad Gateway', context);

    expect(error.kind).toBe('request-failed');
    expect(error.message).toContain('Bad Gateway');
  });
});

describe(apiMessage.name, () => {
  it('reads the CDF error envelope', () => {
    expect(apiMessage('{"error":{"code":400,"message":"nope"}}')).toBe('nope');
  });

  it('falls back to the trimmed text', () => {
    expect(apiMessage('  plain  ')).toBe('plain');
  });
});

describe(networkFailure.name, () => {
  it('keeps the cause in the message', () => {
    const error = networkFailure('Records query', new TypeError('Failed to fetch'));

    expect(error).toMatchObject({ kind: 'network' });
    expect(error.message).toContain('Failed to fetch');
  });
});

describe(isBackendError.name, () => {
  it('recognises backend errors only', () => {
    expect(isBackendError(new BackendError('network', 'x'))).toBe(true);
    expect(isBackendError(new Error('x'))).toBe(false);
  });
});
