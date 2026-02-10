import { useEffect, useState } from "react";
import { normalizeCapability } from "@/shared/permissions-utils";
import { useI18n } from "@/shared/i18n";
import type {
  DataSetSummary,
  GroupSummary,
  LoadState,
  SpaceSummary,
} from "./types";

type UsePermissionsDataArgs = {
  isDuneLoading: boolean;
  sdk: {
    project: string;
    post: Function;
    groups: { list: Function };
    spaces: { list: Function };
  };
};

export function usePermissionsData({ isDuneLoading, sdk }: UsePermissionsDataArgs) {
  const { t } = useI18n();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [groups, setGroups] = useState<GroupSummary[]>([]);
  const [capabilityNames, setCapabilityNames] = useState<string[]>([]);
  const [dataSets, setDataSets] = useState<DataSetSummary[]>([]);
  const [dataSetAccess, setDataSetAccess] = useState<Record<number, string[]>>({});
  const [spaces, setSpaces] = useState<SpaceSummary[]>([]);
  const [spaceAccess, setSpaceAccess] = useState<Record<string, string[]>>({});

  useEffect(() => {
    if (isDuneLoading) return;
    let cancelled = false;

    const loadPermissions = async () => {
      setStatus("loading");
      setErrorMessage(null);
      try {
        const groupResponse = (await sdk.groups.list({ all: true })) as GroupSummary[];
        const datasetResponse = (await sdk.post(
          `/api/v1/projects/${sdk.project}/datasets/list`,
          { data: { limit: 1000 } }
        )) as { data?: { items?: DataSetSummary[] } };
        const spaceItems = await sdk.spaces
          .list({ includeGlobal: true, limit: 1000 })
          .autoPagingToArray();
        const datasets = (datasetResponse.data?.items ?? []) as DataSetSummary[];
        const datasetMap = new Map<number, DataSetSummary>(datasets.map((ds) => [ds.id, ds]));

        const utilized = new Set<string>();
        const accessMap: Record<number, string[]> = {};
        const spaceMap: Record<string, string[]> = {};

        for (const group of groupResponse) {
          for (const cap of group.capabilities ?? []) {
            const normalized = normalizeCapability(cap);
            utilized.add(normalized.name);
            const scope = normalized.scope ?? {};
            const datasetScope = scope["datasetScope"] as { ids?: number[] } | undefined;
            if (datasetScope?.ids) {
              for (const datasetId of datasetScope.ids) {
                if (!accessMap[datasetId]) {
                  accessMap[datasetId] = [];
                }
                const groupLabel = group.name ?? t("permissions.group.fallback", { id: group.id });
                if (!accessMap[datasetId].includes(groupLabel)) {
                  accessMap[datasetId].push(groupLabel);
                }
              }
            }
            const idScope = scope["idScope"] as { ids?: number[] } | undefined;
            if (idScope?.ids) {
              for (const datasetId of idScope.ids) {
                const resolved = datasetMap.get(datasetId);
                if (!resolved) continue;
                if (!accessMap[resolved.id]) {
                  accessMap[resolved.id] = [];
                }
                const groupLabel = group.name ?? t("permissions.group.fallback", { id: group.id });
                if (!accessMap[resolved.id].includes(groupLabel)) {
                  accessMap[resolved.id].push(groupLabel);
                }
              }
            }
            const spaceIdScope = scope["spaceIdScope"] as { spaceIds?: string[] } | undefined;
            if (spaceIdScope?.spaceIds) {
              for (const spaceId of spaceIdScope.spaceIds) {
                if (!spaceMap[spaceId]) {
                  spaceMap[spaceId] = [];
                }
                const groupLabel = group.name ?? t("permissions.group.fallback", { id: group.id });
                if (!spaceMap[spaceId].includes(groupLabel)) {
                  spaceMap[spaceId].push(groupLabel);
                }
              }
            }
          }
        }

        if (!cancelled) {
          setGroups(
            [...groupResponse].sort((a, b) =>
              (a.name ?? a.id).toString().localeCompare((b.name ?? b.id).toString())
            )
          );
          setCapabilityNames(Array.from(utilized).sort((a, b) => a.localeCompare(b)));
          setDataSets(datasets);
          setDataSetAccess(accessMap);
          setSpaces(spaceItems as SpaceSummary[]);
          setSpaceAccess(spaceMap);
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : t("permissions.error"));
          setStatus("error");
        }
      }
    };

    loadPermissions();
    return () => {
      cancelled = true;
    };
  }, [isDuneLoading, sdk, t]);

  return {
    status,
    errorMessage,
    groups,
    capabilityNames,
    dataSets,
    dataSetAccess,
    spaces,
    spaceAccess,
  };
}
