/**
 * Group properties by the container they are stored in (like original NEAT Python).
 * Properties without a container are grouped under "View".
 */

import type { PropertyInfo } from "@/types/dataModel";

const VIEW_KEY = "__view__";

export interface ContainerSection {
  /** Display label: "View" or container name (externalId) */
  label: string;
  /** Container externalId or VIEW_KEY for view-stored props */
  containerKey: string;
  props: PropertyInfo[];
}

function nameAndId(displayName: string | undefined, externalId: string): string {
  if (!externalId) return "—";
  if (displayName) return `${displayName} (${externalId})`;
  return externalId;
}

/**
 * Group a list of properties by their container. View-stored properties (no container) come first.
 */
export function groupPropertiesByContainer(
  props: PropertyInfo[],
  allViews: Record<string, { name: string; display_name?: string }>
): ContainerSection[] {
  const byContainer = new Map<string, PropertyInfo[]>();
  for (const p of props) {
    const key = p.container?.trim() ? p.container : VIEW_KEY;
    if (!byContainer.has(key)) byContainer.set(key, []);
    byContainer.get(key)!.push(p);
  }
  const keys = Array.from(byContainer.keys()).sort((a, b) => {
    if (a === VIEW_KEY) return -1;
    if (b === VIEW_KEY) return 1;
    return a.localeCompare(b);
  });
  return keys.map((containerKey) => {
    const sectionProps = byContainer.get(containerKey)!;
    const label =
      containerKey === VIEW_KEY
        ? "View"
        : nameAndId(allViews[containerKey]?.display_name, containerKey);
    return { label, containerKey, props: sectionProps };
  });
}
