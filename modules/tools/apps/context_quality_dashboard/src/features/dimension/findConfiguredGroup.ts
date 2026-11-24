import type { ConfiguredDataModelDto, ConfiguredGroupDto } from '../../types';

export function findConfiguredGroup(
  models: readonly ConfiguredDataModelDto[],
  groupId: string
): ConfiguredGroupDto | undefined {
  for (const model of models) {
    const group = model.groups.find((entry) => entry.id === groupId);
    if (group !== undefined) {
      return group;
    }
  }
  return undefined;
}
