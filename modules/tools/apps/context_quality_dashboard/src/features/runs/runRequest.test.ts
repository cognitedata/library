import { describe, expect, it } from 'vitest';

import { requestKey, requestNeedsConfigFile, toRunInput, type RunRequest } from './runRequest';

const MODELS = [{ model: 'cdf_cdm:CogniteCore:v1', links: ['CogniteAsset.type'] }];

describe(toRunInput.name, () => {
  it('runs everything: every dimension and the configuration file', () => {
    expect(toRunInput({ kind: 'everything' }, MODELS)).toEqual({
      extraLinks: MODELS,
      scope: { dimensions: 'all', configured: true },
    });
  });

  it('runs every dimension without the configuration file, even when there is one', () => {
    expect(toRunInput({ kind: 'dimensions' }, MODELS)).toEqual({
      extraLinks: [],
      scope: { dimensions: 'all', configured: false },
    });
  });

  it('runs only the configured metrics: no dimension', () => {
    expect(toRunInput({ kind: 'configured' }, MODELS)).toEqual({
      extraLinks: MODELS,
      scope: { dimensions: [], configured: true },
    });
  });

  it('runs one dimension and nothing else', () => {
    expect(toRunInput({ kind: 'dimension', dimensionId: 'asset-hierarchy' }, null)).toEqual({
      extraLinks: [],
      scope: { dimensions: ['asset-hierarchy'], configured: false },
    });
  });

  it('runs every metric in one configured view', () => {
    const links = [
      { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteEquipment.equipmentClass' },
      { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteEquipment.files' },
    ];
    expect(
      toRunInput(
        {
          kind: 'configuredView',
          groupId: 'cdf_cdm:CogniteCore:v1/CogniteEquipment',
          viewLabel: 'CogniteEquipment',
          links,
        },
        [{ model: 'cdf_cdm:CogniteCore:v1', links: ['CogniteEquipment.equipmentClass', 'CogniteEquipment.files'] }]
      )
    ).toEqual({
      extraLinks: [
        {
          model: 'cdf_cdm:CogniteCore:v1',
          links: ['CogniteEquipment.equipmentClass', 'CogniteEquipment.files'],
        },
      ],
      scope: { dimensions: [], configured: true },
    });
  });

  it('runs exactly the picked dimensions and links, grouped by model, each link once', () => {
    const input = toRunInput(
      {
        kind: 'selection',
        dimensionIds: ['files', 'asset-hierarchy'],
        links: [
          { model: 'a:B:v1', link: 'V.p' },
          { model: 'c:D:v1', link: 'W.q' },
          { model: 'a:B:v1', link: 'V.r' },
          { model: 'a:B:v1', link: 'V.p' },
        ],
      },
      MODELS
    );

    expect(input).toEqual({
      extraLinks: [
        { model: 'a:B:v1', links: ['V.p', 'V.r'] },
        { model: 'c:D:v1', links: ['W.q'] },
      ],
      scope: { dimensions: ['files', 'asset-hierarchy'], configured: true },
    });
  });

  it('runs picked dimensions alone without configured metrics or a configuration file', () => {
    expect(toRunInput({ kind: 'selection', dimensionIds: ['files'], links: [] }, null)).toEqual({
      extraLinks: [],
      scope: { dimensions: ['files'], configured: false },
    });
  });

  it('refuses a selection of nothing', () => {
    expect(() => toRunInput({ kind: 'selection', dimensionIds: [], links: [] }, MODELS)).toThrow('Nothing');
  });

  it('refuses a run of the configuration file when there is none', () => {
    expect(() => toRunInput({ kind: 'everything' }, null)).toThrow('configuration file');
    expect(() => toRunInput({ kind: 'configured' }, null)).toThrow('configuration file');
    expect(() =>
      toRunInput({ kind: 'selection', dimensionIds: [], links: [{ model: 'a:B:v1', link: 'V.p' }] }, null)
    ).toThrow('configuration file');
  });

  it('copies the links so that the run input does not share them with the file', () => {
    const input = toRunInput({ kind: 'everything' }, MODELS);

    expect(input.extraLinks[0]?.links).not.toBe(MODELS[0]?.links);
  });
});

describe(requestNeedsConfigFile.name, () => {
  it('is true only for the runs that measure the configuration file', () => {
    const needs = (request: RunRequest) => requestNeedsConfigFile(request);

    expect(needs({ kind: 'everything' })).toBe(true);
    expect(needs({ kind: 'configured' })).toBe(true);
    expect(needs({ kind: 'selection', dimensionIds: [], links: [{ model: 'a:B:v1', link: 'V.p' }] })).toBe(true);
    expect(
      needs({
        kind: 'configuredView',
        groupId: 'g',
        viewLabel: 'V',
        links: [{ model: 'a:B:v1', link: 'V.p' }],
      })
    ).toBe(true);
    expect(needs({ kind: 'selection', dimensionIds: ['files'], links: [] })).toBe(false);
    expect(needs({ kind: 'dimensions' })).toBe(false);
    expect(needs({ kind: 'dimension', dimensionId: 'files' })).toBe(false);
  });
});

describe(requestKey.name, () => {
  it('names a request', () => {
    expect(requestKey({ kind: 'everything' })).toBe('everything');
    expect(requestKey({ kind: 'selection', dimensionIds: [], links: [] })).toBe('selection');
    expect(requestKey({ kind: 'dimension', dimensionId: 'model-3d' })).toBe('dimension-model-3d');
    expect(
      requestKey({
        kind: 'configuredView',
        groupId: 'a:Model:v1/Widget',
        viewLabel: 'Widget',
        links: [],
      })
    ).toBe('configured-view-a:Model:v1/Widget');
  });
});
