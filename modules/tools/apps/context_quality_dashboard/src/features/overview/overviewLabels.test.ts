import { describe, expect, it } from 'vitest';

import {
  criticalityGroupLabelKey,
  criticalityMeaningKey,
  formatSignedPts,
  qualityProgressClassName,
  qualityTextClassName,
  qualityToneFromGap,
} from './overviewLabels';

describe('overviewLabels', () => {
  it('maps criticality groups to section title and meaning copy keys', () => {
    expect(criticalityGroupLabelKey('critical')).toBe('overview.criticalityGroup.critical');
    expect(criticalityGroupLabelKey('important')).toBe('overview.criticalityGroup.important');
    expect(criticalityGroupLabelKey('standard')).toBe('overview.criticalityGroup.standard');
    expect(criticalityMeaningKey('critical')).toBe('overview.criticalityMeaning.critical');
    expect(criticalityMeaningKey('important')).toBe('overview.criticalityMeaning.important');
    expect(criticalityMeaningKey('standard')).toBe('overview.criticalityMeaning.standard');
  });

  it('formats signed point gaps without coloring', () => {
    expect(formatSignedPts(-41, 'pts')).toBe('−41 pts');
    expect(formatSignedPts(11, 'pts')).toBe('+11 pts');
    expect(formatSignedPts(0, 'pts')).toBe('0 pts');
  });

  it('maps quality gaps to tone bands', () => {
    expect(qualityToneFromGap(-41, 34)).toBe('critical');
    expect(qualityToneFromGap(-15, 80)).toBe('weak');
    expect(qualityToneFromGap(5, 95)).toBe('good');
  });

  it('maps quality tones to text classes', () => {
    expect(qualityTextClassName('critical')).toContain('destructive');
    expect(qualityTextClassName('weak')).toContain('warning');
    expect(qualityTextClassName('good')).toContain('success');
  });

  it('uses foreground tokens for progress fills so bars read darker than muted backgrounds', () => {
    expect(qualityProgressClassName('critical')).toContain('destructive-foreground');
    expect(qualityProgressClassName('weak')).toContain('warning-foreground');
    expect(qualityProgressClassName('good')).toContain('success-foreground');
  });
});
