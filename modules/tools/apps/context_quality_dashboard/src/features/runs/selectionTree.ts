import type { RunConfigModel } from '../configuration/runConfigFile';

/** One configured metric: a `View.property` link of a data model, with an id that is stable across renders. */
export type MetricNode = {
  id: string;
  model: string;
  view: string;
  property: string;
  link: string;
};

export type ViewNode = { id: string; view: string; metrics: MetricNode[] };
export type ModelNode = { model: string; views: ViewNode[] };

/** The tri-state of a parent checkbox. */
export type CheckState = 'checked' | 'unchecked' | 'indeterminate';

export function metricId(model: string, link: string): string {
  return `${model}|${link}`;
}

export function viewNodeId(model: string, view: string): string {
  return `${model}|${view}`;
}

/** Groups the links of the validated configuration by data model, then view. Each metric appears once. */
export function buildSelectionTree(models: readonly RunConfigModel[] | null): ModelNode[] {
  const seen = new Set<string>();
  const tree: ModelNode[] = [];
  for (const entry of models ?? []) {
    let node = tree.find((candidate) => candidate.model === entry.model);
    if (node === undefined) {
      node = { model: entry.model, views: [] };
      tree.push(node);
    }
    for (const link of entry.links) {
      const id = metricId(entry.model, link);
      const [view = '', property = ''] = link.split('.');
      if (seen.has(id)) {
        continue;
      }
      seen.add(id);
      let viewNode = node.views.find((candidate) => candidate.view === view);
      if (viewNode === undefined) {
        viewNode = { id: viewNodeId(entry.model, view), view, metrics: [] };
        node.views.push(viewNode);
      }
      viewNode.metrics.push({ id, model: entry.model, view, property, link });
    }
  }
  return tree;
}

export function allMetrics(tree: readonly ModelNode[]): MetricNode[] {
  return tree.flatMap((node) => node.views.flatMap((view) => view.metrics));
}

export function checkState(ids: readonly string[], selected: ReadonlySet<string>): CheckState {
  const count = ids.filter((id) => selected.has(id)).length;
  if (count === 0) {
    return 'unchecked';
  }
  return count === ids.length ? 'checked' : 'indeterminate';
}

/** Checking a parent selects all of its items; an unchecked or indeterminate parent becomes checked. */
export function toggleMany(ids: readonly string[], selected: ReadonlySet<string>): Set<string> {
  const next = new Set(selected);
  const select = checkState(ids, selected) !== 'checked';
  for (const id of ids) {
    if (select) {
      next.add(id);
    } else {
      next.delete(id);
    }
  }
  return next;
}

/** Drops selected ids that no longer exist, for instance after the configuration file changed. */
export function pruneSelection(selected: ReadonlySet<string>, available: readonly string[]): Set<string> {
  const known = new Set(available);
  return new Set([...selected].filter((id) => known.has(id)));
}
