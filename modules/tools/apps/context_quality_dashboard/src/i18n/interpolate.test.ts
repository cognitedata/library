import { describe, expect, it } from 'vitest';

import { interpolate } from './interpolate';

describe(interpolate.name, () => {
  it('fills {{name}} placeholders', () => {
    expect(interpolate('{{a}} of {{b}}', { a: '1', b: '2' })).toBe('1 of 2');
  });

  it('uses the fallback for a missing value instead of printing the placeholder', () => {
    expect(interpolate('{{a}} items', {}, 'some')).toBe('some items');
  });
});
