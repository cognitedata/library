import { describe, expect, it } from 'vitest';

import {
  DEFAULT_RELATIONSHIP_CONTAINER,
  resolveBackendConfig,
  type BackendConfigResult,
} from './streamConfig';

const CDF_ENV = {
  VITE_QUALITY_BACKEND: 'cdf',
  VITE_CQ_STREAM_ID: 'cq_stream',
  VITE_CQ_SCHEMA_SPACE: 'cq_schema',
};

describe(resolveBackendConfig.name, () => {
  describe('backend selection', () => {
    it('is a configuration error when nothing is selected', () => {
      const result = resolveBackendConfig({});

      expect(issueVariables(result)).toEqual(['VITE_QUALITY_BACKEND']);
      expect(problems(result)).toEqual(['missing']);
    });

    it('treats a blank selection as not set', () => {
      expect(problems(resolveBackendConfig({ VITE_QUALITY_BACKEND: '   ' }))).toEqual(['missing']);
    });

    it('rejects a value that is neither cdf nor mock', () => {
      const result = resolveBackendConfig({ VITE_QUALITY_BACKEND: 'demo' });

      expect(problems(result)).toEqual(['invalid']);
      expect(messages(result)[0]).toContain('"demo"');
    });

    it('accepts mock only when it is named explicitly', () => {
      expect(resolveBackendConfig({ VITE_QUALITY_BACKEND: 'mock' })).toEqual({ ok: true, config: { backend: 'mock' } });
    });
  });

  describe('cdf resource configuration', () => {
    it('requires the stream and the schema space and has no default for either', () => {
      const result = resolveBackendConfig({ VITE_QUALITY_BACKEND: 'cdf' });

      expect(issueVariables(result)).toEqual(['VITE_CQ_STREAM_ID', 'VITE_CQ_SCHEMA_SPACE']);
      expect(problems(result)).toEqual(['missing', 'missing']);
    });

    it('reports which one is missing', () => {
      const result = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_STREAM_ID: undefined });

      expect(issueVariables(result)).toEqual(['VITE_CQ_STREAM_ID']);
    });

    it('rejects identifiers with characters the API would not accept', () => {
      const result = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_STREAM_ID: 'bad/stream', VITE_CQ_SCHEMA_SPACE: 'with space' });

      expect(issueVariables(result)).toEqual(['VITE_CQ_STREAM_ID', 'VITE_CQ_SCHEMA_SPACE']);
      expect(problems(result)).toEqual(['invalid', 'invalid']);
    });

    it('can name the workflow a run is started with, which needs both its id and its version', () => {
      const named = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_WORKFLOW_ID: 'cq-wf', VITE_CQ_WORKFLOW_VERSION: 'v2' });
      const onlyId = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_WORKFLOW_ID: 'cq-wf' });
      const onlyVersion = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_WORKFLOW_VERSION: 'v2' });
      const bad = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_WORKFLOW_ID: 'a/b', VITE_CQ_WORKFLOW_VERSION: 'v2' });

      expect(named.ok && named.config.backend === 'cdf' ? named.config.workflow : undefined).toEqual({
        externalId: 'cq-wf',
        version: 'v2',
      });
      expect(issueVariables(onlyId)).toEqual(['VITE_CQ_WORKFLOW_VERSION']);
      expect(issueVariables(onlyVersion)).toEqual(['VITE_CQ_WORKFLOW_ID']);
      expect(issueVariables(bad)).toEqual(['VITE_CQ_WORKFLOW_ID']);
    });

    it('keeps the contract container name unless it is overridden', () => {
      const result = resolveBackendConfig(CDF_ENV);

      expect(result).toEqual({
        ok: true,
        config: {
          backend: 'cdf',
          stream: {
            streamId: 'cq_stream',
            schemaSpace: 'cq_schema',
            relationshipContainer: DEFAULT_RELATIONSHIP_CONTAINER,
            windowDays: null,
          },
          workflow: null,
        },
      });
    });

    it('takes the container override and the history window when they are valid', () => {
      const result = resolveBackendConfig({
        ...CDF_ENV,
        VITE_CQ_RELATIONSHIP_CONTAINER: 'OtherContainer',
        VITE_CQ_WINDOW_DAYS: '30',
      });

      expect(result).toMatchObject({
        ok: true,
        config: { stream: { relationshipContainer: 'OtherContainer', windowDays: 30 } },
      });
    });

    it.each(['abc', '0', '-3', '2.5', '366'])('rejects the history window "%s" instead of silently falling back', (value) => {
      const result = resolveBackendConfig({ ...CDF_ENV, VITE_CQ_WINDOW_DAYS: value });

      expect(issueVariables(result)).toEqual(['VITE_CQ_WINDOW_DAYS']);
      expect(problems(result)).toEqual(['invalid']);
    });

    it('reports every problem at once', () => {
      const result = resolveBackendConfig({ VITE_QUALITY_BACKEND: 'cdf', VITE_CQ_WINDOW_DAYS: 'x' });

      expect(issueVariables(result)).toEqual(['VITE_CQ_STREAM_ID', 'VITE_CQ_SCHEMA_SPACE', 'VITE_CQ_WINDOW_DAYS']);
    });
  });
});

function issues(result: BackendConfigResult) {
  return result.ok ? [] : result.issues;
}

function issueVariables(result: BackendConfigResult): string[] {
  return issues(result).map((issue) => issue.variable);
}

function problems(result: BackendConfigResult): string[] {
  return issues(result).map((issue) => issue.problem);
}

function messages(result: BackendConfigResult): string[] {
  return issues(result).map((issue) => issue.message);
}
