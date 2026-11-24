import { describe, expect, it } from 'vitest';

import { translate } from './messages';

describe('i18n translate', () => {
  it('returns Japanese strings when available', () => {
    expect(translate('nav.overview', 'ja')).toBe('概要');
  });

  it('falls back to English when a Japanese key is missing', () => {
    expect(translate('nav.overview', 'en')).toBe('Overview');
    expect(translate('missing.key', 'ja')).toBe('missing.key');
    expect(translate('nav.overview', 'ja')).not.toBe('Overview');
  });
});
