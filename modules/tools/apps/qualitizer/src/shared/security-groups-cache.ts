import type { CogniteClient } from "@cognite/sdk";
import { LRUCache } from "lru-cache";

export type SecurityGroupListItem = {
  id: number;
  name?: string;
  sourceId?: string;
  capabilities?: Array<Record<string, unknown>>;
};

export const SECURITY_GROUPS_LIST_CACHE_MAX = 100;
export const SECURITY_GROUPS_LIST_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

const groupsListCache = new LRUCache<string, SecurityGroupListItem[]>({
  max: SECURITY_GROUPS_LIST_CACHE_MAX,
  ttl: SECURITY_GROUPS_LIST_CACHE_TTL_MS,
});

function listKey(projectUrlName: string): string {
  return `cdf:${projectUrlName}:groups:list:all`;
}

export async function cachedSecurityGroupsList(
  sdkForProject: CogniteClient,
  projectUrlName: string
): Promise<SecurityGroupListItem[]> {
  const key = listKey(projectUrlName);
  const hit = groupsListCache.get(key);
  if (hit) return hit;
  const raw = (await sdkForProject.groups.list({ all: true })) as SecurityGroupListItem[];
  groupsListCache.set(key, raw);
  return raw;
}

export type SecurityGroupsListCacheStatRow = {
  id: string;
  label: string;
  size: number;
  max: number;
  fillRate: number;
  ttlMs: number;
  calculatedSize: number;
  maxSize: number;
};

export function getSecurityGroupsListCacheStats(): SecurityGroupsListCacheStatRow {
  return {
    id: "securityGroups.list",
    label: "Security groups definitions (groups/list per CDF project)",
    size: groupsListCache.size,
    max: groupsListCache.max,
    fillRate: groupsListCache.max > 0 ? groupsListCache.size / groupsListCache.max : 0,
    ttlMs: SECURITY_GROUPS_LIST_CACHE_TTL_MS,
    calculatedSize: groupsListCache.calculatedSize ?? 0,
    maxSize: groupsListCache.maxSize ?? groupsListCache.max,
  };
}
