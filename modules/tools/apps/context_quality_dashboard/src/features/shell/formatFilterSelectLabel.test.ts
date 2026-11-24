import { describe, expect, it } from 'vitest';

import {
  FILTER_ALL,
  formatFilterSelectLabel,
  fromFilterSelectValue,
  toFilterSelectValue,
} from './formatFilterSelectLabel';

describe(formatFilterSelectLabel.name, () => {
  const options = [
    { id: 'demo-central', label: 'Demo Central' },
    { id: 'demo-north', label: 'Demo North' },
  ];

  it('returns the all label for the sentinel value', () => {
    expect(formatFilterSelectLabel(FILTER_ALL, options, 'All')).toBe('All');
  });

  it('returns the matching option label', () => {
    expect(formatFilterSelectLabel('demo-central', options, 'All')).toBe('Demo Central');
  });

  it('falls back to the all label for unknown values', () => {
    expect(formatFilterSelectLabel('missing', options, 'All')).toBe('All');
  });
});

describe(toFilterSelectValue.name, () => {
  it('maps undefined to the sentinel value', () => {
    expect(toFilterSelectValue(undefined)).toBe(FILTER_ALL);
  });
});

describe(fromFilterSelectValue.name, () => {
  it('maps the sentinel value back to undefined', () => {
    expect(fromFilterSelectValue(FILTER_ALL)).toBeUndefined();
  });
});
