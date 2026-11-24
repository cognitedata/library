import type { DimensionId } from '../../types';

import { ALL_DIMENSION_IDS } from './dimensionIds';

export type DimensionsScopeKind = 'builtin' | 'configured';

export type BuiltinDimensionsScope = {
  kind: 'builtin';
  dimensionId: DimensionId;
};

export type ConfiguredDimensionsScope = {
  kind: 'configured';
  modelId: string;
  groupId: string;
};

export type DimensionsScope = BuiltinDimensionsScope | ConfiguredDimensionsScope;

const BUILTIN_PREFIX = 'builtin:';
const CONFIGURED_PREFIX = 'configured:';

export function encodeDimensionsScopeValue(scope: DimensionsScope): string {
  if (scope.kind === 'builtin') {
    return `${BUILTIN_PREFIX}${scope.dimensionId}`;
  }
  return `${CONFIGURED_PREFIX}${scope.groupId}`;
}

export function decodeDimensionsScopeValue(
  value: string,
  modelIdForGroup?: string
): DimensionsScope | undefined {
  if (value.startsWith(BUILTIN_PREFIX)) {
    const dimensionId = value.slice(BUILTIN_PREFIX.length);
    if (!isBuiltinDimensionId(dimensionId)) {
      return undefined;
    }
    return { kind: 'builtin', dimensionId };
  }
  if (value.startsWith(CONFIGURED_PREFIX)) {
    const groupId = value.slice(CONFIGURED_PREFIX.length);
    if (groupId.length === 0) {
      return undefined;
    }
    return {
      kind: 'configured',
      groupId,
      modelId: modelIdForGroup ?? '',
    };
  }
  return undefined;
}

export function dimensionsScopeSearchValue(parts: {
  dimensionLabel?: string;
  modelLabel?: string;
  viewLabel?: string;
  groupId?: string;
}): string {
  return [parts.dimensionLabel, parts.modelLabel, parts.viewLabel, parts.groupId]
    .filter((part) => part !== undefined && part.length > 0)
    .join(' ')
    .toLowerCase();
}

export function isBuiltinDimensionId(value: string): value is DimensionId {
  return ALL_DIMENSION_IDS.some((id) => id === value);
}
