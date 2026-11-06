import { describe, expect, it } from 'vitest';

import { appContentHorizontalInsetClassName } from './appContentHorizontalInset';

describe('appContentHorizontalInsetClassName', () => {
  it('should use fluid padding with a 90rem content cap', () => {
    expect(appContentHorizontalInsetClassName).toContain('90rem');
    expect(appContentHorizontalInsetClassName).toContain('max(1rem');
  });
});
