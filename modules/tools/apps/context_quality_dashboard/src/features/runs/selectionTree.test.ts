import { describe, expect, it } from 'vitest';

import {
  allMetrics,
  buildSelectionTree,
  checkState,
  metricId,
  pruneSelection,
  toggleMany,
} from './selectionTree';

const MODELS = [
  { model: 'a:B:v1', links: ['Asset.name', 'Asset.parent', 'Pump.tag'] },
  { model: 'c:D:v1', links: ['Asset.name'] },
  { model: 'a:B:v1', links: ['Asset.name', 'Pump.rating'] },
];

describe(buildSelectionTree.name, () => {
  it('groups by data model, then view, and counts each metric once', () => {
    const tree = buildSelectionTree(MODELS);

    expect(tree.map((node) => node.model)).toEqual(['a:B:v1', 'c:D:v1']);
    expect(tree[0]?.views.map((view) => [view.view, view.metrics.map((metric) => metric.property)])).toEqual([
      ['Asset', ['name', 'parent']],
      ['Pump', ['tag', 'rating']],
    ]);
    expect(allMetrics(tree)).toHaveLength(5);
  });

  it('keeps the same link in two models as two metrics', () => {
    const ids = allMetrics(buildSelectionTree(MODELS)).map((metric) => metric.id);

    expect(ids).toContain(metricId('a:B:v1', 'Asset.name'));
    expect(ids).toContain(metricId('c:D:v1', 'Asset.name'));
  });

  it('is empty without a configuration', () => {
    expect(buildSelectionTree(null)).toEqual([]);
  });
});

describe(checkState.name, () => {
  it('is checked, unchecked or indeterminate', () => {
    expect(checkState(['a', 'b'], new Set())).toBe('unchecked');
    expect(checkState(['a', 'b'], new Set(['a']))).toBe('indeterminate');
    expect(checkState(['a', 'b'], new Set(['a', 'b', 'z']))).toBe('checked');
  });
});

describe(toggleMany.name, () => {
  it('selects all when not fully selected, keeping other selections', () => {
    expect([...toggleMany(['a', 'b'], new Set(['a', 'z']))].sort()).toEqual(['a', 'b', 'z']);
  });

  it('clears only its own items when fully selected', () => {
    expect([...toggleMany(['a', 'b'], new Set(['a', 'b', 'z']))]).toEqual(['z']);
  });
});

describe(pruneSelection.name, () => {
  it('drops ids that are no longer available', () => {
    expect([...pruneSelection(new Set(['a', 'gone']), ['a', 'b'])]).toEqual(['a']);
  });
});
