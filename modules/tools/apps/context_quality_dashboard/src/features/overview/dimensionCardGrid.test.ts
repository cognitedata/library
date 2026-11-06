import { describe, expect, it } from 'vitest';

import { dimensionCardGridClassName } from './dimensionCardGrid';

describe(dimensionCardGridClassName, () => {
  it('matches the overview dimension card breakpoint layout', () => {
    expect(dimensionCardGridClassName).toContain('xl:grid-cols-4');
    expect(dimensionCardGridClassName).toContain('sm:grid-cols-2');
    expect(dimensionCardGridClassName).toContain('auto-rows-fr');
  });
});
