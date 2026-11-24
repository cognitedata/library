import { describe, expect, it } from 'vitest';

import type { ConfiguredGroupDto } from '../types';

import { groupConfiguredMetricsByModel } from './configuredMetricsByModel';

describe(groupConfiguredMetricsByModel.name, () => {
  it('groups view cards by data model id without mixing models', () => {
    const groups: ConfiguredGroupDto[] = [
      group('cdf_cdm:CogniteCore:v1', 'CogniteCore', 'CogniteAsset'),
      group('cdf_cdm:CogniteCore:v1', 'CogniteCore', 'CogniteEquipment'),
      group('my_space:MyModel:v2', 'MyModel', 'Widget'),
    ];

    const models = groupConfiguredMetricsByModel(groups);

    expect(models).toHaveLength(2);
    expect(models[0]?.id).toBe('cdf_cdm:CogniteCore:v1');
    expect(models[0]?.groups).toHaveLength(2);
    expect(models[1]?.id).toBe('my_space:MyModel:v2');
    expect(models[1]?.groups).toHaveLength(1);
  });
});

function group(modelId: string, modelLabel: string, viewLabel: string): ConfiguredGroupDto {
  return {
    id: `${modelId}/${viewLabel}`,
    modelId,
    modelLabel,
    viewLabel,
    metrics: [{ label: 'type', modelId, view: viewLabel, totalCount: 1, linkedCount: 1, coveragePercent: 100 }],
  };
}
