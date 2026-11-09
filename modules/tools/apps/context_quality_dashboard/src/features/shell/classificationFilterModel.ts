export type ClassificationFilterState = {
  kind?: string;
  value?: string;
};

export type Classified = {
  classes: Record<string, string>;
};

export type ClassificationValueOption = {
  value: string;
  count: number;
};

const KIND_ORDER: readonly string[] = ['assetClass', 'assetType', 'equipmentType', 'fileCategory'];

function kindRank(kind: string): number {
  const index = KIND_ORDER.indexOf(kind);
  return index === -1 ? KIND_ORDER.length : index;
}

/** Kinds that at least one item carries, in the order the overview lists them. */
export function classificationKinds(items: readonly Classified[]): string[] {
  const kinds = new Set(items.flatMap((item) => Object.keys(item.classes)));
  return [...kinds].sort((left, right) => kindRank(left) - kindRank(right) || left.localeCompare(right));
}

/** Values of one kind with how many items have each, largest first. */
export function classificationValues(items: readonly Classified[], kind: string): ClassificationValueOption[] {
  const counts = new Map<string, number>();
  for (const item of items) {
    const value = item.classes[kind];
    if (value !== undefined) {
      counts.set(value, (counts.get(value) ?? 0) + 1);
    }
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort((left, right) => right.count - left.count || left.value.localeCompare(right.value));
}

/** A filter only applies once both a kind and a value are chosen. */
export function matchesClassification(item: Classified, filter: ClassificationFilterState): boolean {
  if (filter.kind === undefined || filter.value === undefined) {
    return true;
  }
  return item.classes[filter.kind] === filter.value;
}

/** Kind names are translated when a key exists; otherwise the raw kind is shown as words. */
export function classificationKindLabel(kind: string, t: (key: string) => string): string {
  const key = `overview.classification.kind.${kind}`;
  const translated = t(key);
  if (translated !== key) {
    return translated;
  }
  const words = kind.replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
}
