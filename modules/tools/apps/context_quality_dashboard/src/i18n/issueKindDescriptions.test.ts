import { describe, expect, it } from 'vitest';

import en from './en.json';
import ja from './ja.json';

const KIND_PREFIX = 'issues.kind.';
const DESCRIPTION_PREFIX = 'issues.kindDescription.';

describe('issue kind descriptions', () => {
  it.each([
    ['English', en],
    ['Japanese', ja],
  ] as const)('%s describes every issue kind that has a name', (_language, messages) => {
    const keys = Object.keys(messages);
    const kinds = keys.filter((key) => key.startsWith(KIND_PREFIX)).map((key) => key.slice(KIND_PREFIX.length));

    expect(kinds.length).toBeGreaterThan(0);
    for (const kind of kinds) {
      expect(keys, `description for ${kind}`).toContain(`${DESCRIPTION_PREFIX}${kind}`);
    }
  });
});
