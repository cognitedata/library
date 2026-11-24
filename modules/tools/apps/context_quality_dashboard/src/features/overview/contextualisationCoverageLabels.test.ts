import { describe, expect, it } from 'vitest';

import {
  formatCount,
  formatSignedCount,
} from './contextualisationCoverageLabels';

describe(formatCount.name, () => {
  it('formats thousands with commas', () => {
    expect(formatCount(3600)).toBe('3,600');
    expect(formatCount(320)).toBe('320');
  });
});

describe(formatSignedCount.name, () => {
  it('signs a positive count', () => {
    expect(formatSignedCount(11)).toBe('+11');
  });

  it('signs a negative count with the absolute value', () => {
    expect(formatSignedCount(-5)).toBe('−5');
  });

  it('renders zero without a sign', () => {
    expect(formatSignedCount(0)).toBe('0');
  });
});
