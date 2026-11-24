import { describe, expect, it } from 'vitest';

import { placementForDimensionTrendTooltip } from './dimensionTrendTooltipLayout';

describe(placementForDimensionTrendTooltip.name, () => {
  it('anchors the tooltip to the right when the point is near the plot end', () => {
    const placement = placementForDimensionTrendTooltip(280, 10);

    expect(placement.transform).toBe('translate(-100%, 0)');
    expect(placement.left).toBe('100%');
  });

  it('anchors the tooltip to the left when the point is near the plot start', () => {
    const placement = placementForDimensionTrendTooltip(26, 10);

    expect(placement.transform).toBe('translate(0, 0)');
  });

  it('places the tooltip below high values so it does not cover the line at the top', () => {
    const placement = placementForDimensionTrendTooltip(140, 12);

    expect(Number.parseFloat(placement.top)).toBeGreaterThan(12);
  });
});
