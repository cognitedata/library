import { describe, expect, it } from 'vitest';

import { DEFAULT_HOST_SYNCED_STATE } from './defaultState';
import { parseInitialState } from './parseInitialState';

describe(parseInitialState.name, () => {
  it('returns default application state when host state is missing', () => {
    expect(parseInitialState(undefined)).toEqual(DEFAULT_HOST_SYNCED_STATE);
  });

  it('applies valid host initial state', () => {
    const parsed = parseInitialState(
      JSON.stringify({
        page: 'alerts',
        filters: { facilityId: 'demo-north' },
        trendWindow: '7d',
      })
    );

    expect(parsed.page).toBe('alerts');
    expect(parsed.filters.facilityId).toBe('demo-north');
    expect(parsed.trendWindow).toBe('7d');
  });

  it('keeps only the space filter from host state and ignores legacy slice keys', () => {
    const parsed = parseInitialState(
      JSON.stringify({
        filters: {
          facilityId: 'demo-north',
          divisionId: 'prod-a',
          disciplineId: 'mechanical',
          ownerId: 'alice',
        },
      })
    );

    expect(parsed.filters).toEqual({ facilityId: 'demo-north' });
  });

  it('restores the issues page from host state', () => {
    const parsed = parseInitialState(JSON.stringify({ page: 'issues' }));

    expect(parsed.page).toBe('issues');
  });

  it('falls back safely for invalid host initial state', () => {
    expect(parseInitialState('{not-json')).toEqual(DEFAULT_HOST_SYNCED_STATE);
    expect(
      parseInitialState(JSON.stringify({ page: 'invalid-page', trendWindow: '1y' }))
    ).toEqual({
      ...DEFAULT_HOST_SYNCED_STATE,
      filters: {},
    });
  });
});

describe('dimensions scope in the initial state', () => {
  it('restores configured view selection from host state', () => {
    const parsed = parseInitialState(
      JSON.stringify({
        page: 'dimensions',
        dimensionsScope: 'configured',
        configuredModelId: 'cdf_cdm:CogniteCore:v1',
        configuredGroupId: 'cdf_cdm:CogniteCore:v1/CogniteAsset',
      })
    );

    expect(parsed.dimensionsScope).toBe('configured');
    expect(parsed.configuredGroupId).toBe('cdf_cdm:CogniteCore:v1/CogniteAsset');
  });
});

describe('run notes panel in the initial state', () => {
  it('restores an open panel and ignores anything that is not true', () => {
    expect(parseInitialState(JSON.stringify({ page: 'overview', runNotesOpen: true })).runNotesOpen).toBe(true);
    expect(parseInitialState(JSON.stringify({ page: 'overview', runNotesOpen: 'yes' })).runNotesOpen).toBeUndefined();
    expect(parseInitialState(JSON.stringify({ page: 'overview' })).runNotesOpen).toBeUndefined();
  });
});
