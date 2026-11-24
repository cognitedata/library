import { describe, expect, it } from 'vitest';

import {
  sectionHeadingClassName,
  sectionHeadingDestructiveClassName,
  sectionHeadingSpacedClassName,
} from './sectionHeading';

describe('sectionHeading', () => {
  it('uses text-base font-semibold for section titles', () => {
    expect(sectionHeadingClassName).toContain('text-base');
    expect(sectionHeadingClassName).toContain('font-semibold');
    expect(sectionHeadingSpacedClassName).toContain('mb-3');
    expect(sectionHeadingDestructiveClassName).toContain('text-destructive-foreground');
  });
});
