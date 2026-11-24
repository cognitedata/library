import { describe, expect, it } from 'vitest';

import { locationOptions } from './useAppShellViewModel';

const location = (id: string, name: string, parentId: string | null) => ({ id, name, parentId, facilityIds: [id] });

describe(locationOptions.name, () => {
  it('lists each parent before its children, indenting the children', () => {
    const options = locationOptions([
      location('b-site', 'B site', 'region'),
      location('region', 'Region', null),
      location('a-site', 'A site', 'region'),
      location('central', 'Central', null),
    ]);

    expect(options.map((option) => option.id)).toEqual(['central', 'region', 'a-site', 'b-site']);
    expect(options[1]?.label).toBe('Region');
    expect(options[2]?.label).toContain('A site');
    expect(options[2]?.label).not.toBe('A site');
  });

  it('treats a child whose parent is not offered as a root', () => {
    expect(locationOptions([location('site', 'Site', 'gone')]).map((option) => option.label)).toEqual(['Site']);
  });

  it('does not loop on a parent cycle', () => {
    expect(locationOptions([location('a', 'A', 'b'), location('b', 'B', 'a')])).toEqual([]);
  });
});
