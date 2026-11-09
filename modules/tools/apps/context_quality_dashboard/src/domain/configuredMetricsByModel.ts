import type { ConfiguredDataModelDto, ConfiguredGroupDto } from '../types';

/** Groups view-level metric cards under their data model (`space:externalId:version`). */
export function groupConfiguredMetricsByModel(
  groups: readonly ConfiguredGroupDto[]
): ConfiguredDataModelDto[] {
  const byModelId = new Map<string, ConfiguredDataModelDto>();

  for (const group of groups) {
    const existing = byModelId.get(group.modelId);
    if (existing !== undefined) {
      existing.groups.push(group);
      continue;
    }
    byModelId.set(group.modelId, {
      id: group.modelId,
      label: group.modelLabel,
      groups: [group],
    });
  }

  return [...byModelId.values()].sort((left, right) => left.label.localeCompare(right.label));
}
