import { describe, expect, it } from 'vitest';

import { isMetricId, STANDARD_METRIC_ID_COUNT } from './metricIds';

describe('standard metric IDs', () => {
  it('keeps the locked standard metric contract at 61 metric IDs', () => {
    expect(STANDARD_METRIC_ID_COUNT).toBe(61);
  });

  it('does not treat custom rule IDs as standard metric IDs', () => {
    expect(isMetricId('customQualityRule')).toBe(false);
    expect(isMetricId('quality-rule-idi-bom-maintenance-order')).toBe(false);
    expect(isMetricId('hierarchyCompletionRate')).toBe(true);
  });
});
