import { describe, expect, it } from 'vitest';

import type { ConfiguredGroupDto } from '../../types';

import { configuredGroupRunRequest, selectedLinksToRunConfigModels } from './configuredGroupRunRequest';
import { toRunInput } from './runRequest';

describe(configuredGroupRunRequest.name, () => {
  const group: ConfiguredGroupDto = {
    id: 'cdf_cdm:CogniteCore:v1/CogniteAsset',
    modelId: 'cdf_cdm:CogniteCore:v1',
    modelLabel: 'CogniteCore',
    viewLabel: 'CogniteAsset',
    metrics: [
      {
        label: 'assetClass',
        modelId: 'cdf_cdm:CogniteCore:v1',
        view: 'CogniteAsset',
        totalCount: 10,
        linkedCount: 9,
        coveragePercent: 90,
      },
      {
        label: 'type',
        modelId: 'cdf_cdm:CogniteCore:v1',
        view: 'CogniteAsset',
        totalCount: 10,
        linkedCount: 8,
        coveragePercent: 80,
      },
    ],
  };

  it('builds a configured-view run for all metrics in the view', () => {
    const { request, models } = configuredGroupRunRequest(group);

    expect(request).toEqual({
      kind: 'configuredView',
      groupId: group.id,
      viewLabel: 'CogniteAsset',
      links: [
        { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteAsset.assetClass' },
        { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteAsset.type' },
      ],
    });
    expect(models).toEqual([
      {
        model: 'cdf_cdm:CogniteCore:v1',
        links: ['CogniteAsset.assetClass', 'CogniteAsset.type'],
      },
    ]);
    expect(toRunInput(request, models)).toEqual({
      extraLinks: [
        {
          model: 'cdf_cdm:CogniteCore:v1',
          links: ['CogniteAsset.assetClass', 'CogniteAsset.type'],
        },
      ],
      scope: { dimensions: [], configured: true },
    });
  });
});

describe(selectedLinksToRunConfigModels.name, () => {
  it('deduplicates links per model', () => {
    expect(
      selectedLinksToRunConfigModels([
        { model: 'a:B:v1', link: 'V.p' },
        { model: 'a:B:v1', link: 'V.p' },
        { model: 'c:D:v1', link: 'W.q' },
      ])
    ).toEqual([
      { model: 'a:B:v1', links: ['V.p'] },
      { model: 'c:D:v1', links: ['W.q'] },
    ]);
  });
});
