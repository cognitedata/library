import type { DimensionId, Insight  } from '../types';

const ENABLED_DIMENSIONS: readonly DimensionId[] = [
  'asset-hierarchy',
  'equipment-asset',
  'time-series',
  'maintenance',
  'file-annotation',
  'model-3d',
  'files',
];

export const MOCK_INSIGHTS: readonly Insight[] = ENABLED_DIMENSIONS.flatMap((dimensionId) => [
  {
    id: `insight-rule-${dimensionId}`,
    dimensionId,
    source: 'rule',
    messageKey: `insight.rule.${dimensionId}`,
    mentionsTier: 'all',
    facilityId: null,
  },
  {
    id: `insight-ai-${dimensionId}`,
    dimensionId,
    source: 'ai',
    messageKey: `insight.ai.${dimensionId}`,
    mentionsTier: 1,
    facilityId: 'demo-north',
  },
]);
