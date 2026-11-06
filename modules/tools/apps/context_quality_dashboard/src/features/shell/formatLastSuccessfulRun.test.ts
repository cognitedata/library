import { describe, expect, it } from 'vitest';

import { formatLastSuccessfulRun } from './formatLastSuccessfulRun';

describe(formatLastSuccessfulRun.name, () => {
  it('formats an ISO timestamp with the provided locale', () => {
    const formatted = formatLastSuccessfulRun('2026-09-01T03:30:00.000Z', 'en-GB');

    expect(formatted).toContain('2026');
    expect(formatted).toContain('03:30');
  });
});
