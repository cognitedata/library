import { describe, expect, it } from 'vitest';

import { bootLoadingDescription } from './loadingSteps';

describe(bootLoadingDescription.name, () => {
  it('should describe mock service preparation differently from cdf', () => {
    expect(bootLoadingDescription('prepare-services', { backend: 'mock' })).toContain('sample data');
    expect(bootLoadingDescription('prepare-services', { backend: 'cdf', stream: {
      streamId: 's',
      schemaSpace: 'sp',
      relationshipContainer: 'Run',
      windowDays: null,
    }, workflow: null })).toContain('deployment settings');
  });

  it('should describe fusion and cdf auth steps', () => {
    expect(bootLoadingDescription('connect-fusion')).toContain('Fusion');
    expect(bootLoadingDescription('authenticate-cdf')).toContain('Signing in');
  });
});
