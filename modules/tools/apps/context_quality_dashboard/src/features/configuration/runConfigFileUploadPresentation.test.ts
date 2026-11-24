import { describe, expect, it } from 'vitest';

import { formatRunConfigFileSize, runConfigFileByteLength } from './runConfigFileUploadPresentation';

describe(formatRunConfigFileSize.name, () => {
  it('formats bytes and kilobytes', () => {
    expect(formatRunConfigFileSize(500)).toBe('500 B');
    expect(formatRunConfigFileSize(2048)).toBe('2 KB');
  });
});

describe(runConfigFileByteLength.name, () => {
  it('counts UTF-8 bytes', () => {
    expect(runConfigFileByteLength('a')).toBe(1);
    expect(runConfigFileByteLength('é')).toBe(2);
  });
});
