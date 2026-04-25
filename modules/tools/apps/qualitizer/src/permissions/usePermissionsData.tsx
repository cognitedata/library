import type { CogniteClient } from "@cognite/sdk";
import { useEffect, useState } from "react";
import { normalizeCapability } from "@/shared/permissions-utils";
import { cachedSecurityGroupsList } from "@/shared/security-groups-cache";
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
  const [loadingDetail, setLoadingDetail] = useState<string | null>(null);

  useEffect(() => {
    if (isDuneLoading) return;
    let cancelled = false;

    const loadPermissions = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setLoadingDetail(t("permissions.loadingDetail.groups"));
      try {
        const groupResponse = (await cachedSecurityGroupsList(
          sdk as CogniteClient,
          sdk.project
        )) as GroupSummary[];
        if (cancelled) return;
        setLoadingDetail(t("permissions.loadingDetail.datasets"));
        const datasetResponse = (await sdk.post(
          `/api/v1/projects/${sdk.project}/datasets/list`,
          { data: { limit: 1000 } }
        )) as { data?: { items?: DataSetSummary[] } };
        if (cancelled) return;
        setLoadingDetail(t("permissions.loadingDetail.spacesStarting"));
        const spaceItems: SpaceSummary[] = [];
        let spaceCursor: string | undefined;
        let spacePage = 0;
        do {
          const spaceResponse = await sdk.spaces.list({
            includeGlobal: true,
            limit: 100,
            cursor: spaceCursor,
          }) as { items?: SpaceSummary[]; nextCursor?: string | null };
          spaceItems.push(...(spaceResponse.items ?? []));
          spaceCursor = spaceResponse.nextCursor ?? undefined;
          spacePage += 1;
          if (!cancelled) {
            setLoadingDetail(
              t("permissions.loadingDetail.spaces", { count: spaceItems.length, page: spacePage })
            );
          }
        } while (spaceCursor);
        const datasets = (datasetResponse.data?.items ?? []) as DataSetSummary[];
        const datasetMap = new Map<number, DataSetSummary>(datasets.map((ds) => [ds.id, ds]));

        const utilized = new Set<string>();
        const accessMap: Record<number, string[]> = {};
        const spaceMap: Record<string, string[]> = {};

        const groupTotal = groupResponse.length;
        let groupIndex = 0;
        for (const group of groupResponse) {
          if (!cancelled && (groupIndex % 20 === 0 || groupIndex === groupTotal - 1)) {
            setLoadingDetail(
              t("permissions.loadingDetail.analyzing", {
                current: groupIndex + 1,
                total: groupTotal,
              })
            );
          }
          groupIndex += 1;
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
          setLoadingDetail(null);
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
          setLoadingDetail(null);
          setErrorMessage(error instanceof Error ? error.message : t("permissions.error"));
          setStatus("error");
        }
      }
    };

    loadPermissions();
    return () => {
      cancelled = true;
      setLoadingDetail(null);
    };
  }, [isDuneLoading, sdk, t]);

  return {
    status,
    errorMessage,
    loadingDetail,
    groups,
    capabilityNames,
    dataSets,
    dataSetAccess,
    spaces,
    spaceAccess,
  };
}
