import type { ConfiguredMetricKey } from '../../types';

/** Stable string key for React state, query keys, and chart test ids. */
export function configuredMetricKeyId(key: ConfiguredMetricKey): string {
  return `${key.modelId}/${key.view}/${key.property}`;
}

export function configuredMetricChartId(key: ConfiguredMetricKey): string {
  return configuredMetricKeyId(key).replace(/[^a-zA-Z0-9_-]/g, '_');
}
