import { describe, expect, it } from 'vitest';

/**
 * Guards for the boundary between this dashboard and the backend it reads. The backend (streams, schemas,
 * containers, views, functions, workflows, groups) is deployed and owned elsewhere, so nothing here may create
 * or change it, and the sample data must stay out of the path that reads a deployed backend.
 */
const sources = import.meta.glob<string>(['/src/**/*.ts', '/src/**/*.tsx'], {
  query: '?raw',
  import: 'default',
  eager: true,
});

function productionSources(): [string, string][] {
  return Object.entries(sources).filter(
    ([path]) =>
      !/\.test\.tsx?$/.test(path) && !path.includes('/test/') && !path.includes('/mock/') && !path.endsWith('vitest.setup.ts')
  );
}

function importsOf(text: string): string[] {
  return [...text.matchAll(/^import\s[^;]*?from\s+'([^']+)'/gm)].map((match) => match[1] ?? '');
}

describe('the sample data stays out of the backend path', () => {
  it('has production sources to check', () => {
    expect(productionSources().length).toBeGreaterThan(100);
  });

  it('is not imported statically by any production module', () => {
    const offenders = productionSources().flatMap(([path, text]) =>
      importsOf(text)
        .filter((specifier) => /(^|\/)mock(\/|$)/.test(specifier))
        .map((specifier) => `${path} imports ${specifier}`)
    );

    expect(offenders).toEqual([]);
  });

  it('is reached only through one on-demand import in the services factory', () => {
    const dynamicImports = productionSources().flatMap(([path, text]) =>
      [...text.matchAll(/import\(\s*'([^']+)'\s*\)/g)]
        .map((match) => `${path} -> ${match[1]}`)
        .filter((entry) => /mock/.test(entry))
    );

    expect(dynamicImports).toEqual(['/src/services/createServices.ts -> ./mock/createMockServices']);
  });
});

/**
 * The one thing the dashboard may start, and only when the deployment names a workflow: a run of the already deployed
 * metrics workflow, as the signed-in user. It creates and changes no resource.
 */
const RUN_SERVICE = '/src/services/cdf/HttpRunService.ts';
/** The one other thing it asks for: a signed download link to the workbook of unlinked items a run wrote to Files. */
const EXPORT_SERVICE = '/src/services/cdf/HttpExportService.ts';

describe('the dashboard only reads the backend', () => {
  const apiPaths = productionSources().flatMap(([path, text]) =>
    [...text.matchAll(/\/api\/v1\/projects\/\$\{[^}]*\}\/([^`'"]*)/g)].map((match) => `${path}: ${match[1]}`)
  );

  it('calls exactly the stream lookup, the records filter, the status and the list of runs and a download link', () => {
    expect(apiPaths.sort()).toEqual([
      `${EXPORT_SERVICE}: files/downloadlink`,
      `${RUN_SERVICE}: workflows/executions/${'$'}{encodeURIComponent(executionId)}`,
      `${RUN_SERVICE}: workflows/executions/list`,
      '/src/services/cdf/recordsClient.ts: streams/${encodeURIComponent(config.streamId)}/records/filter',
      '/src/services/cdf/streamPreflight.ts: streams/${encodeURIComponent(config.streamId)}',
    ].sort());
  });

  it('starts a run with exactly two calls: opening a session and running the named workflow version', () => {
    const text = sources[RUN_SERVICE] ?? '';
    const posts = [...text.matchAll(/\$\{root\}\/([^`'"]*)/g)].map((match) => match[1]);

    expect(posts).toEqual([
      'sessions',
      'workflows/${encodeURIComponent(externalId)}/versions/${encodeURIComponent(version)}/run',
    ]);
  });

  it('never uses a verb that changes or deletes anything', () => {
    const offenders = productionSources().flatMap(([path, text]) =>
      [...text.matchAll(/method:\s*'(PUT|PATCH|DELETE)'/g)].map((match) => `${path} uses ${match[1]}`)
    );

    expect(offenders).toEqual([]);
  });

  it('posts only to the records filter, to start a run and to ask for a download link', () => {
    const posters = productionSources()
      .filter(([, text]) => /method:\s*'POST'/.test(text))
      .map(([path]) => path);

    expect(posters).toEqual([EXPORT_SERVICE, RUN_SERVICE, '/src/services/cdf/recordsClient.ts']);
  });

  it('does not name any resource it would have to provision', () => {
    const offenders = productionSources()
      .filter(([path]) => path !== RUN_SERVICE)
      .flatMap(([path, text]) =>
      [...text.matchAll(/\/(models\/(spaces|containers|views|datamodels)|functions|workflows|transformations|groups|streams\/records)\b/g)].map(
        (match) => `${path} mentions ${match[0]}`
      )
    );

    expect(offenders).toEqual([]);
  });
});
