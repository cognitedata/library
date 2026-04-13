import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { select } from "d3-selection";
import { line } from "d3-shape";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { extractDataModelRefs } from "@/transformations/transformationChecks";
import { fetchTransformationsByIds } from "@/transformations/fetchTransformationsByIds";
import { getDataModelUrl, getTransformationPreviewUrl } from "@/shared/cdf-browser-url";
import { useI18n } from "@/shared/i18n";
import { compareVersionStrings, isChecksumLikeVersion } from "./versioning-utils";
import { GRID_VERSION_HEADER_HEIGHT, VersioningGridScroll } from "./VersioningGridScroll";
import { DataCatalogVersionHistory } from "./DataCatalogVersionHistory";

const ROW_HEIGHT = 36;
const COL_WIDTH = 56;
const LABEL_WIDTH = 200;
const PADDING = 20;
const SMALL_R = 3;
const LARGE_R = 6;

type DataModelVersionItem = {
  space: string;
  externalId: string;
  version: string;
  name?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  description?: string;
  views?: unknown;
};

type DataModelRow = {
  key: string;
  label: string;
  versions: Map<string, DataModelVersionItem>;
};

type CellDot = {
  x: number;
  y: number;
  r: number;
  fill: string;
  dmKey?: string;
  version?: string;
  label?: string;
  space?: string;
  externalId?: string;
  borderTone?: "green" | "red" | null;
} | null;

type DmGridDrawRow = {
  label: string;
  dmKey: string;
  versionCount: number;
  space: string;
  externalId: string;
  latestVersion: string;
  dots: CellDot[];
  connected: Array<{ x: number; y: number }>;
};

const LABEL_NAME_MAX_CHARS = 26;

function truncateGridLabel(s: string): string {
  if (s.length <= LABEL_NAME_MAX_CHARS) return s;
  return `${s.slice(0, LABEL_NAME_MAX_CHARS - 1)}…`;
}

type ReferrerItem =
  | { type: "dataModel"; label: string; url?: string }
  | { type: "transformation"; id: string; name: string; url?: string };

function dataModelFingerprint(m: {
  name?: string;
  description?: string;
  views?: unknown;
}): string {
  const raw = m.views;
  const viewEntries: Array<{ space: string; externalId: string; version?: string }> = [];
  if (Array.isArray(raw)) {
    for (const v of raw) {
      if (v && typeof v === "object" && "space" in v && "externalId" in v) {
        const o = v as { space: string; externalId: string; version?: string };
        viewEntries.push({ space: o.space, externalId: o.externalId, version: o.version });
      }
    }
  }
  viewEntries.sort((a, b) =>
    `${a.space}:${a.externalId}:${a.version ?? ""}`.localeCompare(
      `${b.space}:${b.externalId}:${b.version ?? ""}`
    )
  );
  return JSON.stringify({
    name: m.name ?? "",
    description: m.description ?? "",
    views: viewEntries,
  });
}

const DEST_DM_VERSION_UNSPECIFIED = "__dest_dm_latest__";

type DmGridLegendFilterId =
  | "sizeSmall"
  | "sizeLarge"
  | "latestInUse"
  | "latestNotInUse"
  | "olderNotInUse"
  | "otherInUse"
  | "txLatestBorder"
  | "txOlderBorder";

function computeDmGridCell(
  row: DataModelRow,
  ver: string,
  filteredVersions: string[],
  detailsMap: Map<string, DataModelVersionItem>,
  dmKeysInCatalog: Set<string>,
  dmKeysInTransformation: Set<string>,
  txByCell: ReadonlyMap<string, unknown>
): {
  rVal: number;
  fill: string;
  borderTone: "green" | "red" | null;
  legendFlags: Set<DmGridLegendFilterId>;
} | null {
  const item = row.versions.get(ver);
  if (!item) return null;

  const rowVersionsOrdered = filteredVersions.filter((v) => row.versions.has(v));
  const latestVersion = rowVersionsOrdered[rowVersionsOrdered.length - 1];
  const inCatalog = dmKeysInCatalog.has(row.key);
  const inTransformation = dmKeysInTransformation.has(row.key);
  const inUse = inCatalog || inTransformation;

  const key = `${row.key}:${ver}`;
  const details = detailsMap.get(key) ?? item;
  const idxInRow = rowVersionsOrdered.indexOf(ver);
  const prevVerInRow = idxInRow > 0 ? rowVersionsOrdered[idxInRow - 1] : null;
  const prevDetails = prevVerInRow
    ? detailsMap.get(`${row.key}:${prevVerInRow}`) ?? row.versions.get(prevVerInRow) ?? null
    : null;

  let rVal: number;
  if (!prevDetails) {
    rVal = LARGE_R;
  } else {
    const currFp = dataModelFingerprint(details);
    const prevFp = dataModelFingerprint(prevDetails);
    rVal = currFp === prevFp ? SMALL_R : LARGE_R;
  }

  const isLatestVersion = ver === latestVersion;
  let fill: string;
  if (isLatestVersion && inUse) {
    fill = "#22c55e";
  } else if (isLatestVersion && !inUse) {
    fill = "#ea580c";
  } else if (!isLatestVersion && !inUse) {
    fill = "#ec4899";
  } else {
    fill = "white";
  }

  const cellKey = `${row.key}:${ver}`;
  const destLatestKey = `${row.key}:${DEST_DM_VERSION_UNSPECIFIED}`;
  const txHasDestinationHere =
    txByCell.has(cellKey) || (isLatestVersion && txByCell.has(destLatestKey));
  const borderTone: "green" | "red" | null =
    txHasDestinationHere && isLatestVersion ? "green" : txHasDestinationHere && !isLatestVersion ? "red" : null;

  const legendFlags = new Set<DmGridLegendFilterId>();
  if (rVal === SMALL_R) legendFlags.add("sizeSmall");
  if (rVal === LARGE_R) legendFlags.add("sizeLarge");
  if (isLatestVersion && inUse) legendFlags.add("latestInUse");
  else if (isLatestVersion && !inUse) legendFlags.add("latestNotInUse");
  else if (!isLatestVersion && !inUse) legendFlags.add("olderNotInUse");
  else legendFlags.add("otherInUse");
  if (borderTone === "green") legendFlags.add("txLatestBorder");
  if (borderTone === "red") legendFlags.add("txOlderBorder");

  return { rVal, fill, borderTone, legendFlags };
}

function dmRowLegendFlagUnion(
  row: DataModelRow,
  filteredVersions: string[],
  detailsMap: Map<string, DataModelVersionItem>,
  dmKeysInCatalog: Set<string>,
  dmKeysInTransformation: Set<string>,
  txByCell: ReadonlyMap<string, unknown>
): Set<DmGridLegendFilterId> {
  const union = new Set<DmGridLegendFilterId>();
  for (const ver of filteredVersions) {
    const cell = computeDmGridCell(
      row,
      ver,
      filteredVersions,
      detailsMap,
      dmKeysInCatalog,
      dmKeysInTransformation,
      txByCell
    );
    if (cell) for (const id of cell.legendFlags) union.add(id);
  }
  return union;
}

const DATA_MODEL_VERSION_LEGEND_ENTRIES: Array<{
  id: DmGridLegendFilterId;
  swatch: ReactNode;
  label: string;
}> = [
  {
    id: "sizeSmall",
    swatch: (
      <span className="inline-flex h-2 w-2 shrink-0 rounded-full border border-slate-300 bg-white" />
    ),
    label: "Small = no change from previous version",
  },
  {
    id: "sizeLarge",
    swatch: (
      <span className="inline-flex h-3 w-3 shrink-0 rounded-full border border-slate-300 bg-white" />
    ),
    label: "Large = change from previous version",
  },
  {
    id: "latestInUse",
    swatch: <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full bg-green-500" />,
    label: "Latest version, in use",
  },
  {
    id: "latestNotInUse",
    swatch: <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full bg-orange-500" />,
    label: "Latest version, not in use",
  },
  {
    id: "olderNotInUse",
    swatch: <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full bg-pink-500" />,
    label: "Older version, not in use",
  },
  {
    id: "otherInUse",
    swatch: (
      <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full border border-slate-300 bg-white" />
    ),
    label: "Other (older, in use)",
  },
  {
    id: "txLatestBorder",
    swatch: (
      <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full border-2 border-indigo-600 bg-transparent" />
    ),
    label: "Write destination: latest data model version (indigo ring)",
  },
  {
    id: "txOlderBorder",
    swatch: (
      <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full border-2 border-red-600 bg-transparent" />
    ),
    label: "Write destination: older data model version (red ring)",
  },
];

const TAB_THRESHOLD = 10;
const STROKE_TX_LATEST = "#4f46e5";

type TransformationDestinationDataModel = {
  space?: string;
  externalId?: string;
  version?: string;
  destinationType?: string;
};

type TransformationApiItem = {
  id: number | string;
  name?: string;
  query?: string;
  destination?: { dataModel?: TransformationDestinationDataModel };
};

type DataModelOption = { key: string; baseKey: string; label: string; viewKeys: Set<string> };

const DM_LIST_PAGE_LIMIT = 250;
const DM_DETAILS_BATCH = 50;

function countUniqueDataModelKeys(items: DataModelVersionItem[]): number {
  const s = new Set<string>();
  for (const i of items) s.add(`${i.space}:${i.externalId}`);
  return s.size;
}

export function DataModelVersions() {
  const { t } = useI18n();
  const headerSvgRef = useRef<SVGSVGElement | null>(null);
  const bodySvgRef = useRef<SVGSVGElement | null>(null);
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { dataModels, dataModelsStatus, loadDataModels, retrieveDataModels } = useAppData();
  const [status, setStatus] = useState<"idle" | "loading" | "success" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loadProgress, setLoadProgress] = useState<
    | { phase: "listing"; itemsLoaded: number; uniqueModels: number }
    | { phase: "details"; batchIndex: number; batchTotal: number }
    | null
  >(null);
  const [dmRows, setDmRows] = useState<DataModelRow[]>([]);
  const [versions, setVersions] = useState<string[]>([]);
  const [detailsMap, setDetailsMap] = useState<Map<string, DataModelVersionItem>>(new Map());
  const [modelOptions, setModelOptions] = useState<DataModelOption[]>([]);
  const [selectedModelKey, setSelectedModelKey] = useState<string | null>(null);
  const [dmKeysInTransformation, setDmKeysInTransformation] = useState<Set<string>>(new Set());
  const [transformationRefsByModelVersion, setTransformationRefsByModelVersion] = useState<
    Map<string, Array<{ id: string; name: string }>>
  >(new Map());
  const [pinnedBubble, setPinnedBubble] = useState<{
    dmKey: string;
    version: string;
    label: string;
    space: string;
    externalId: string;
  } | null>(null);
  const [modelVersionRefs, setModelVersionRefs] = useState<
    Map<string, { versions: Set<string>; hasUnspecified: boolean }>
  >(new Map());
  const [dmTxByCell, setDmTxByCell] = useState<Map<string, Array<{ id: string; name: string }>>>(
    new Map()
  );
  const [dmLegendFilter, setDmLegendFilter] = useState<DmGridLegendFilterId | null>(null);
  const [showChecksumVersions, setShowChecksumVersions] = useState(false);
  const [versionHistoryDmKey, setVersionHistoryDmKey] = useState<string | null>(null);

  useEffect(() => {
    if (!isSdkLoading) loadDataModels();
  }, [isSdkLoading, loadDataModels]);

  useEffect(() => {
    if (dataModelsStatus !== "success" || dataModels.length === 0) {
      setModelOptions([]);
      return;
    }

    let cancelled = false;
    const loadModelViews = async () => {
      const options: DataModelOption[] = [];
      for (const model of dataModels) {
        const key =
          `${model.space}:${model.externalId}:${model.version ?? "latest"}` as string;
        const baseKey = `${model.space}:${model.externalId}`;
        const label = model.name ?? model.externalId ?? key;

        const viewKeys = new Set<string>();
        const modelViews = (model as { views?: Array<{ space: string; externalId: string }> })
          .views ?? [];
        for (const v of modelViews) {
          viewKeys.add(`${v.space}:${v.externalId}`);
        }

        if (modelViews.length === 0) {
          const response = (await retrieveDataModels(
            [
              {
                space: model.space,
                externalId: model.externalId,
                ...(model.version ? { version: model.version } : {}),
              },
            ],
            { inlineViews: true }
          )) as { items?: Array<{ views?: Array<{ space: string; externalId: string }> }> };
          const item = response.items?.[0];
          const views = item?.views ?? [];
          for (const v of views) {
            viewKeys.add(`${v.space}:${v.externalId}`);
          }
        }

        options.push({ key, baseKey, label, viewKeys });
        if (cancelled) return;
      }

      options.sort((a, b) => a.label.localeCompare(b.label));
      if (!cancelled) setModelOptions(options);
    };

    loadModelViews();
    return () => {
      cancelled = true;
    };
  }, [dataModelsStatus, dataModels, retrieveDataModels]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const load = async () => {
      const destinationDmTxByCell = new Map<string, Array<{ id: string; name: string }>>();
      const pushDestTx = (cellKey: string, txItem: { id: string; name: string }) => {
        let arr = destinationDmTxByCell.get(cellKey);
        if (!arr) {
          arr = [];
          destinationDmTxByCell.set(cellKey, arr);
        }
        if (!arr.some((x) => x.id === txItem.id)) arr.push(txItem);
      };

      try {
        const response = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations`,
          { params: { includePublic: "true", limit: "1000" } }
        )) as { data?: { items?: TransformationApiItem[] } };
        const items = response.data?.items ?? [];
        const idsNeedingDetail: string[] = [];
        for (const t of items) {
          const destination = t.destination;
          const query = t.query ?? "";
          const needDetail =
            !destination?.dataModel?.space ||
            !destination?.dataModel?.externalId ||
            !String(query).trim();
          if (needDetail) idsNeedingDetail.push(String(t.id));
        }
        const detailById = await fetchTransformationsByIds(sdk, sdk.project, idsNeedingDetail);

        const modelRefs = new Set<string>();
        const versionRefs = new Map<string, { versions: Set<string>; hasUnspecified: boolean }>();
        const txByModelVersion = new Map<string, Array<{ id: string; name: string }>>();
        const destinationDmBaseKeys = new Set<string>();

        for (const t of items) {
          if (cancelled) return;
          const txItem = { id: String(t.id), name: t.name ?? String(t.id) };

          let destination = t.destination;
          let query = t.query ?? "";
          const detail = detailById.get(String(t.id));
          if (detail) {
            if (detail.destination?.dataModel?.space && detail.destination?.dataModel?.externalId) {
              destination = { ...destination, dataModel: detail.destination.dataModel };
            }
            if (detail.query != null && String(detail.query).trim() !== "") {
              query = detail.query;
            }
          }

          const dm = destination?.dataModel;
          if (dm?.space && dm?.externalId) {
            const dk = `${dm.space}:${dm.externalId}`;
            destinationDmBaseKeys.add(dk);
            const verPart = dm.version?.trim();
            const versionToken = verPart ? verPart : DEST_DM_VERSION_UNSPECIFIED;
            pushDestTx(`${dk}:${versionToken}`, txItem);
          }

          if (!String(query).trim()) continue;
          const refs = extractDataModelRefs(query);
          for (const ref of refs) {
            const space = ref.space ?? "";
            const externalId = ref.externalId ?? "";
            const key = `${space}:${externalId}`;
            if (!key || key === ":") continue;
            modelRefs.add(key);
            const ver = ref.version?.trim() ?? "";
            let entry = versionRefs.get(key);
            if (!entry) {
              entry = { versions: new Set(), hasUnspecified: false };
              versionRefs.set(key, entry);
            }
            if (ver) {
              entry.versions.add(ver);
            } else {
              entry.hasUnspecified = true;
            }
            const mvKey = `${key}:${ver}`;
            let arr = txByModelVersion.get(mvKey);
            if (!arr) {
              arr = [];
              txByModelVersion.set(mvKey, arr);
            }
            if (!arr.some((x) => x.id === txItem.id)) arr.push(txItem);
          }
        }
        if (!cancelled) {
          setModelVersionRefs(versionRefs);
          setTransformationRefsByModelVersion(txByModelVersion);
          setDmTxByCell(destinationDmTxByCell);
        }
        const dmKeys = new Set<string>([...modelRefs, ...destinationDmBaseKeys]);
        if (!cancelled) setDmKeysInTransformation(dmKeys);
      } catch {
        if (!cancelled) {
          setDmKeysInTransformation(new Set());
          setDmTxByCell(new Map());
        }
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, isSdkLoading]);

  const dmKeysInCatalog = useMemo(() => {
    const set = new Set<string>();
    for (const opt of modelOptions) {
      set.add(opt.baseKey);
    }
    return set;
  }, [modelOptions]);

  useEffect(() => {
    setPinnedBubble(null);
  }, [selectedModelKey]);

  useEffect(() => {
    setDmLegendFilter(null);
  }, [selectedModelKey]);

  useEffect(() => {
    setShowChecksumVersions(false);
  }, [selectedModelKey]);

  const filteredDmRows = useMemo(() => {
    if (!selectedModelKey) return dmRows;
    const opt = modelOptions.find((o) => o.key === selectedModelKey);
    if (!opt) return dmRows;
    return dmRows.filter((row) => row.key === opt.baseKey);
  }, [dmRows, selectedModelKey, modelOptions]);

  const filteredVersions = useMemo(() => {
    if (!selectedModelKey || filteredDmRows.length === 0) return versions;
    const used = new Set<string>();
    for (const row of filteredDmRows) {
      for (const v of row.versions.keys()) used.add(v);
    }
    return versions.filter((v) => used.has(v));
  }, [versions, selectedModelKey, filteredDmRows]);

  const hasChecksumVersions = useMemo(
    () => filteredVersions.some((v) => isChecksumLikeVersion(v)),
    [filteredVersions]
  );

  const gridVersions = useMemo(() => {
    if (showChecksumVersions || !hasChecksumVersions) return filteredVersions;
    return filteredVersions.filter((v) => !isChecksumLikeVersion(v));
  }, [filteredVersions, showChecksumVersions, hasChecksumVersions]);

  const dmRowLegendFlagsByKey = useMemo(() => {
    const m = new Map<string, Set<DmGridLegendFilterId>>();
    for (const row of filteredDmRows) {
      m.set(
        row.key,
        dmRowLegendFlagUnion(
          row,
          gridVersions,
          detailsMap,
          dmKeysInCatalog,
          dmKeysInTransformation,
          dmTxByCell
        )
      );
    }
    return m;
  }, [
    filteredDmRows,
    gridVersions,
    detailsMap,
    dmKeysInCatalog,
    dmKeysInTransformation,
    dmTxByCell,
  ]);

  const legendFilteredDmRows = useMemo(() => {
    if (!dmLegendFilter) return filteredDmRows;
    return filteredDmRows.filter((row) => dmRowLegendFlagsByKey.get(row.key)?.has(dmLegendFilter));
  }, [filteredDmRows, dmLegendFilter, dmRowLegendFlagsByKey]);

  const usedDmLegendIds = useMemo(() => {
    const u = new Set<DmGridLegendFilterId>();
    for (const row of filteredDmRows) {
      const flags = dmRowLegendFlagsByKey.get(row.key);
      if (flags) for (const id of flags) u.add(id);
    }
    return u;
  }, [filteredDmRows, dmRowLegendFlagsByKey]);

  const focusedDataModelRow = useMemo(() => {
    if (!selectedModelKey) return null;
    const opt = modelOptions.find((o) => o.key === selectedModelKey);
    if (!opt) return null;
    return dmRows.find((r) => r.key === opt.baseKey) ?? null;
  }, [selectedModelKey, modelOptions, dmRows]);

  const versionHistoryRow = useMemo(() => {
    if (!versionHistoryDmKey) return null;
    return dmRows.find((r) => r.key === versionHistoryDmKey) ?? null;
  }, [versionHistoryDmKey, dmRows]);

  const versionHistoryVersionsOrdered = useMemo(() => {
    if (!versionHistoryRow) return [];
    return versions.filter((v) => versionHistoryRow.versions.has(v));
  }, [versions, versionHistoryRow]);

  const visibleDmLegendIds = useMemo(() => {
    const u = new Set<DmGridLegendFilterId>();
    const showSizePair = usedDmLegendIds.has("sizeSmall") && usedDmLegendIds.has("sizeLarge");
    if (showSizePair) {
      if (usedDmLegendIds.has("sizeSmall")) u.add("sizeSmall");
      if (usedDmLegendIds.has("sizeLarge")) u.add("sizeLarge");
    }
    for (const id of usedDmLegendIds) {
      if (id === "sizeSmall" || id === "sizeLarge") continue;
      u.add(id);
    }
    return u;
  }, [usedDmLegendIds]);

  useEffect(() => {
    if (dmLegendFilter != null && !visibleDmLegendIds.has(dmLegendFilter)) {
      setDmLegendFilter(null);
    }
  }, [dmLegendFilter, visibleDmLegendIds]);

  const loadData = useCallback(async () => {
    if (isSdkLoading) return;
    setStatus("loading");
    setErrorMessage(null);
    setLoadProgress({ phase: "listing", itemsLoaded: 0, uniqueModels: 0 });
    try {
      const listItems: DataModelVersionItem[] = [];
      let cursor: string | undefined;
      do {
        const response = await sdk.dataModels.list({
          includeGlobal: true,
          allVersions: true,
          limit: DM_LIST_PAGE_LIMIT,
          cursor,
        });
        const items = (response.items ?? []) as Array<{
          space: string;
          externalId: string;
          version?: string;
          name?: string;
          createdTime?: number;
          lastUpdatedTime?: number;
          description?: string;
          views?: unknown;
        }>;
        for (const m of items) {
          listItems.push({
            space: m.space,
            externalId: m.externalId,
            version: String(m.version ?? "latest"),
            name: m.name,
            createdTime: m.createdTime,
            lastUpdatedTime: m.lastUpdatedTime,
            description: m.description,
            views: m.views,
          });
        }
        cursor = response.nextCursor ?? undefined;
        setLoadProgress({
          phase: "listing",
          itemsLoaded: listItems.length,
          uniqueModels: countUniqueDataModelKeys(listItems),
        });
      } while (cursor);

      const dmMap = new Map<string, Map<string, DataModelVersionItem>>();
      const versionSet = new Set<string>();

      for (const item of listItems) {
        const v = item.version;
        versionSet.add(v);
        const key = `${item.space}:${item.externalId}`;
        let verMap = dmMap.get(key);
        if (!verMap) {
          verMap = new Map();
          dmMap.set(key, verMap);
        }
        verMap.set(v, item);
      }

      const allVersions = Array.from(versionSet);
      allVersions.sort((a, b) => {
        const cmp = compareVersionStrings(a, b);
        if (cmp !== 0) return cmp;
        const aItem = listItems.find((i) => i.version === a);
        const bItem = listItems.find((i) => i.version === b);
        const aTime = aItem?.createdTime ?? 0;
        const bTime = bItem?.createdTime ?? 0;
        if (aTime !== bTime) return aTime - bTime;
        return String(a).localeCompare(String(b));
      });

      const refs = listItems
        .filter((i) => i.version != null && i.version !== "")
        .map((i) => ({
          space: i.space,
          externalId: i.externalId,
          version: String(i.version),
        }));

      const details = new Map<string, DataModelVersionItem>();
      const detailBatchTotal = Math.max(1, Math.ceil(refs.length / DM_DETAILS_BATCH));
      for (let i = 0; i < refs.length; i += DM_DETAILS_BATCH) {
        const batchIndex = Math.floor(i / DM_DETAILS_BATCH) + 1;
        setLoadProgress({ phase: "details", batchIndex, batchTotal: detailBatchTotal });
        const batch = refs.slice(i, i + DM_DETAILS_BATCH).map(({ space, externalId, version }) => ({
          space,
          externalId,
          version,
        }));
        const response = (await sdk.dataModels.retrieve(batch as never, {
          inlineViews: true,
        })) as { items?: DataModelVersionItem[] };
        for (let j = 0; j < batch.length; j++) {
          const item = response.items?.[j];
          const ref = batch[j];
          if (!item || !ref) continue;
          const v = String(item.version ?? ref.version ?? "latest");
          const key = `${item.space}:${item.externalId}:${v}`;
          details.set(key, {
            space: item.space,
            externalId: item.externalId,
            version: v,
            name: item.name,
            createdTime: item.createdTime,
            lastUpdatedTime: item.lastUpdatedTime,
            description: item.description,
            views: item.views,
          });
        }
      }

      const rows: DataModelRow[] = [];
      for (const [dmKey, verMap] of dmMap) {
        const first = Array.from(verMap.values())[0];
        const label = first?.name ?? first?.externalId ?? dmKey;
        rows.push({
          key: dmKey,
          label,
          versions: verMap,
        });
      }
      rows.sort((a, b) => a.label.localeCompare(b.label));

      setDmRows(rows);
      setVersions(allVersions);
      setDetailsMap(details);
      setLoadProgress(null);
      setStatus("success");
    } catch (error) {
      setLoadProgress(null);
      setErrorMessage(error instanceof Error ? error.message : "Failed to load data models.");
      setStatus("error");
    }
  }, [sdk, isSdkLoading]);

  useEffect(() => {
    if (!isSdkLoading) loadData();
  }, [isSdkLoading, loadData]);

  const { rows, linePath } = useMemo(() => {
    if (legendFilteredDmRows.length === 0 || gridVersions.length === 0) {
      return { rows: [] as DmGridDrawRow[], linePath: line<{ x: number; y: number }>() };
    }

    const lineGen = line<{ x: number; y: number }>()
      .x((d) => d.x)
      .y((d) => d.y);

    const rows: DmGridDrawRow[] = [];

    for (let r = 0; r < legendFilteredDmRows.length; r++) {
      const row = legendFilteredDmRows[r];
      const colonIdx = row.key.indexOf(":");
      const space = colonIdx >= 0 ? row.key.slice(0, colonIdx) : "";
      const externalId = colonIdx >= 0 ? row.key.slice(colonIdx + 1) : row.key;
      const orderedVers = versions.filter((v) => row.versions.has(v));
      const latestVersion = orderedVers[orderedVers.length - 1] ?? "";
      const cy = PADDING + r * ROW_HEIGHT + ROW_HEIGHT / 2;
      const dots: CellDot[] = [];
      const connected: Array<{ x: number; y: number }> = [];

      for (let c = 0; c < gridVersions.length; c++) {
        const ver = gridVersions[c];
        const cx = LABEL_WIDTH + PADDING + c * COL_WIDTH + COL_WIDTH / 2;

        const cell = computeDmGridCell(
          row,
          ver,
          gridVersions,
          detailsMap,
          dmKeysInCatalog,
          dmKeysInTransformation,
          dmTxByCell
        );
        if (!cell) {
          dots.push(null);
          continue;
        }

        const colonIdx = row.key.indexOf(":");
        const space = colonIdx >= 0 ? row.key.slice(0, colonIdx) : "";
        const externalId = colonIdx >= 0 ? row.key.slice(colonIdx + 1) : row.key;
        dots.push({
          x: cx,
          y: cy,
          r: cell.rVal,
          fill: cell.fill,
          dmKey: row.key,
          version: ver,
          label: row.label,
          space,
          externalId,
          borderTone: cell.borderTone,
        });
        connected.push({ x: cx, y: cy });
      }

      rows.push({
        label: row.label,
        dmKey: row.key,
        versionCount: row.versions.size,
        space,
        externalId,
        latestVersion,
        dots,
        connected,
      });
    }

    return { rows, linePath: lineGen };
  }, [
    legendFilteredDmRows,
    gridVersions,
    versions,
    detailsMap,
    dmKeysInCatalog,
    dmKeysInTransformation,
    dmTxByCell,
  ]);

  const referrers = useMemo((): ReferrerItem[] => {
    if (!pinnedBubble) return [];
    const items: ReferrerItem[] = [];
    items.push({
      type: "dataModel",
      label: `${pinnedBubble.label} ${pinnedBubble.version}`,
      url: getDataModelUrl(
        sdk.project,
        pinnedBubble.space,
        pinnedBubble.externalId,
        pinnedBubble.version
      ),
    });
    const row = dmRows.find((r) => r.key === pinnedBubble.dmKey);
    const orderedVers = versions.filter((v) => row?.versions.has(v) ?? false);
    const latestV = orderedVers[orderedVers.length - 1];
    const cellKey = `${pinnedBubble.dmKey}:${pinnedBubble.version}`;
    const destLatestKey = `${pinnedBubble.dmKey}:${DEST_DM_VERSION_UNSPECIFIED}`;
    const seenTx = new Set<string>();
    const pushDestTxList = (list: Array<{ id: string; name: string }> | undefined) => {
      for (const tx of list ?? []) {
        if (seenTx.has(tx.id)) continue;
        seenTx.add(tx.id);
        items.push({
          type: "transformation",
          id: tx.id,
          name: tx.name,
          url: getTransformationPreviewUrl(sdk.project, tx.id),
        });
      }
    };
    pushDestTxList(dmTxByCell.get(cellKey));
    if (latestV != null && pinnedBubble.version === latestV) {
      pushDestTxList(dmTxByCell.get(destLatestKey));
    }
    const latestVer = orderedVers[orderedVers.length - 1];
    const isPinnedLatest = latestVer != null && pinnedBubble.version === latestVer;
    const pushQueryRefs = (list: Array<{ id: string; name: string }> | undefined) => {
      for (const tx of list ?? []) {
        if (seenTx.has(tx.id)) continue;
        seenTx.add(tx.id);
        items.push({
          type: "transformation",
          id: tx.id,
          name: tx.name,
          url: getTransformationPreviewUrl(sdk.project, tx.id),
        });
      }
    };
    pushQueryRefs(transformationRefsByModelVersion.get(`${pinnedBubble.dmKey}:${pinnedBubble.version}`));
    if (isPinnedLatest) {
      pushQueryRefs(transformationRefsByModelVersion.get(`${pinnedBubble.dmKey}:`));
    }
    return items;
  }, [pinnedBubble, dmTxByCell, transformationRefsByModelVersion, dmRows, versions, sdk.project]);

  const handleDmBubbleClick = useCallback((d: NonNullable<CellDot>) => {
    if (
      d.dmKey != null &&
      d.version != null &&
      d.label != null &&
      d.space != null &&
      d.externalId != null
    ) {
      setPinnedBubble({
        dmKey: d.dmKey,
        version: d.version,
        label: d.label,
        space: d.space,
        externalId: d.externalId,
      });
    }
  }, []);

  const handleRowLabelOpenHistory = useCallback((dmKey: string) => {
    setVersionHistoryDmKey(dmKey);
  }, []);

  useEffect(() => {
    if (!headerSvgRef.current || gridVersions.length === 0) return;
    const versionCount = gridVersions.length;
    const width = LABEL_WIDTH + PADDING * 2 + versionCount * COL_WIDTH;
    const h = GRID_VERSION_HEADER_HEIGHT;
    const root = select(headerSvgRef.current);
    root.selectAll("*").remove();
    root.attr("width", width).attr("height", h).attr("viewBox", `0 0 ${width} ${h}`);
    root
      .append("rect")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", width)
      .attr("height", h)
      .attr("fill", "skyblue");
    root
      .append("g")
      .selectAll("text.version")
      .data(gridVersions)
      .enter()
      .append("text")
      .attr("class", "version")
      .attr("x", (_, i) => LABEL_WIDTH + PADDING + i * COL_WIDTH + COL_WIDTH / 2)
      .attr("y", PADDING - 4)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", "#1e293b")
      .text((d) => d);
  }, [gridVersions]);

  useEffect(() => {
    if (!bodySvgRef.current || rows.length === 0) return;
    const handler = handleDmBubbleClick;
    const openHistory = handleRowLabelOpenHistory;
    const pinned = pinnedBubble;
    const historyTitle = t("dataCatalog.dataModelVersions.tooltipVersionHistory");
    const fusionTitle = t("dataCatalog.dataModelVersions.tooltipFusion");
    const project = sdk.project;
    const versionCount = gridVersions.length;
    const width = LABEL_WIDTH + PADDING * 2 + versionCount * COL_WIDTH;
    const height = PADDING * 2 + rows.length * ROW_HEIGHT - GRID_VERSION_HEADER_HEIGHT;
    const root = select(bodySvgRef.current);
    root.selectAll("*").remove();
    root.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);

    root
      .append("rect")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "skyblue");

    const main = root.append("g").attr("transform", `translate(0, ${-GRID_VERSION_HEADER_HEIGHT})`);

    main
      .selectAll("g.row")
      .data(rows)
      .enter()
      .append("g")
      .attr("class", "row")
      .each(function (rowData, rowIndex) {
        const g = select(this);

        g.append("path")
          .attr("d", linePath(rowData.connected) ?? "")
          .attr("fill", "none")
          .attr("stroke", "rgba(255,255,255,0.8)")
          .attr("stroke-width", 1);

        const circlesData = rowData.dots.filter((d): d is NonNullable<CellDot> => d !== null);
        g.selectAll("circle")
          .data(circlesData)
          .enter()
          .append("circle")
          .attr("cx", (d) => d.x)
          .attr("cy", (d) => d.y)
          .attr("r", (d) => d.r)
          .attr("fill", (d) => d.fill)
          .attr("stroke", (d) => {
            if (pinned && d.dmKey === pinned.dmKey && d.version === pinned.version) {
              return "rgba(59,130,246,0.95)";
            }
            if (d.borderTone === "red") return "#dc2626";
            if (d.borderTone === "green") return STROKE_TX_LATEST;
            return "none";
          })
          .attr("stroke-width", (d) => {
            if (pinned && d.dmKey === pinned.dmKey && d.version === pinned.version) return 2.5;
            if (d.borderTone) return 2;
            return 0;
          })
          .style("cursor", "pointer")
          .on("click", (_ev, d) => {
            if (d.dmKey != null && d.version != null) handler(d);
          });

        const labelY = PADDING + rowIndex * ROW_HEIGHT + ROW_HEIGHT / 2 + 4;
        const labelG = g.append("g").attr("class", "row-label");

        if (rowData.versionCount > 1) {
          labelG
            .append("rect")
            .attr("x", 0)
            .attr("y", PADDING + rowIndex * ROW_HEIGHT)
            .attr("width", LABEL_WIDTH - 2)
            .attr("height", ROW_HEIGHT)
            .attr("fill", "rgba(255,255,255,0.12)")
            .attr("rx", 4)
            .style("cursor", "pointer")
            .append("title")
            .text(historyTitle);
          labelG
            .append("text")
            .attr("x", LABEL_WIDTH - 20)
            .attr("y", labelY)
            .attr("text-anchor", "end")
            .attr("font-size", 11)
            .attr("fill", "#1d4ed8")
            .attr("text-decoration", "underline")
            .style("cursor", "pointer")
            .text(truncateGridLabel(rowData.label))
            .append("title")
            .text(historyTitle);
          labelG
            .style("cursor", "pointer")
            .on("click", (ev: MouseEvent) => {
              const el = ev.target as Element | null;
              if (el?.closest?.("a")) return;
              openHistory(rowData.dmKey);
            });
        } else {
          labelG
            .append("text")
            .attr("x", LABEL_WIDTH - 20)
            .attr("y", labelY)
            .attr("text-anchor", "end")
            .attr("font-size", 11)
            .attr("fill", "#1e293b")
            .text(truncateGridLabel(rowData.label));
        }

        if (rowData.latestVersion) {
          const fusionUrl = getDataModelUrl(
            project,
            rowData.space,
            rowData.externalId,
            rowData.latestVersion
          );
          const fusionA = labelG
            .append("a")
            .attr("href", fusionUrl)
            .attr("target", "_blank")
            .attr("rel", "noopener noreferrer")
            .style("cursor", "pointer")
            .on("click", (ev: MouseEvent) => {
              ev.stopPropagation();
            });
          fusionA.append("title").text(fusionTitle);
          fusionA
            .append("text")
            .attr("x", LABEL_WIDTH - 4)
            .attr("y", labelY)
            .attr("text-anchor", "end")
            .attr("font-size", 11)
            .attr("font-weight", 600)
            .attr("fill", "#2563eb")
            .attr("text-decoration", "underline")
            .text("↗");
        }
      });
  }, [
    rows,
    gridVersions,
    linePath,
    handleDmBubbleClick,
    handleRowLabelOpenHistory,
    pinnedBubble,
    t,
    sdk.project,
  ]);

  const isLoading = isSdkLoading || status === "loading";
  const dmGridContentWidth = LABEL_WIDTH + PADDING * 2 + gridVersions.length * COL_WIDTH;
  const dmGridSvgHeight =
    PADDING * 2 + rows.length * ROW_HEIGHT - GRID_VERSION_HEADER_HEIGHT;
  const showSelect = modelOptions.length > TAB_THRESHOLD;
  const isLoadingModels = dataModelsStatus === "loading" || dataModelsStatus === "idle";

  if (versionHistoryRow && versionHistoryVersionsOrdered.length > 1) {
    return (
      <section className="flex min-w-0 flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-slate-200 bg-white p-2">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
            {t("dataCatalog.subnav.dataModelVersions")}
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
            onClick={() => setVersionHistoryDmKey(null)}
          >
            {t("dataCatalog.versionHistory.backToGrid")}
          </button>
        </div>
        <DataCatalogVersionHistory
          label={versionHistoryRow.label}
          dmKey={versionHistoryRow.key}
          versionsOrdered={versionHistoryVersionsOrdered}
          detailsMap={detailsMap}
          rowVersions={versionHistoryRow.versions}
          dmTxByCell={dmTxByCell}
          dmKeysInCatalog={dmKeysInCatalog}
          dmKeysInTransformation={dmKeysInTransformation}
        />
      </section>
    );
  }

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <p className="text-sm text-slate-500">
          One row per data model; columns are published versions. Indigo/red rings mark transformation
          write destinations using <code className="rounded bg-slate-100 px-1">destination.dataModel</code>.
        </p>
        <p className="text-sm text-slate-600">{t("dataCatalog.dataModelVersions.rowLabelsHint")}</p>
      </header>
      {!isLoadingModels && modelOptions.length > 0 ? (
        <div className="flex flex-col gap-2">
          <span className="text-xs font-medium text-slate-500">Focus data model</span>
          <div className="flex flex-wrap items-center gap-2">
            {showSelect ? (
              <select
                value={selectedModelKey ?? ""}
                onChange={(e) => setSelectedModelKey(e.target.value || null)}
                className="max-w-sm rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-400"
              >
                <option value="">All data models</option>
                {modelOptions.map((opt) => (
                  <option key={opt.key} value={opt.key}>
                    {opt.label}
                  </option>
                ))}
              </select>
            ) : (
              <div className="flex flex-wrap gap-1">
                <button
                  type="button"
                  onClick={() => setSelectedModelKey(null)}
                  className={`rounded-md px-3 py-1.5 text-sm font-medium transition ${
                    selectedModelKey === null
                      ? "bg-slate-900 text-white"
                      : "border border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                  }`}
                >
                  All
                </button>
                {modelOptions.map((opt) => (
                  <button
                    key={opt.key}
                    type="button"
                    onClick={() => setSelectedModelKey(opt.key)}
                    className={`rounded-md px-3 py-1.5 text-sm font-medium transition ${
                      selectedModelKey === opt.key
                        ? "bg-slate-900 text-white"
                        : "border border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
                    }`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            )}
            {focusedDataModelRow && focusedDataModelRow.versions.size > 1 ? (
              <button
                type="button"
                onClick={() => setVersionHistoryDmKey(focusedDataModelRow.key)}
                className="rounded-md border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-50"
              >
                {t("dataCatalog.versionHistory.open")}
              </button>
            ) : null}
          </div>
        </div>
      ) : null}
      <div className="flex items-stretch gap-4">
        <div className="min-w-0 flex-1 rounded-md border border-slate-200">
          {status === "error" ? (
            <div className="flex h-64 items-center justify-center bg-red-50 text-sm text-red-700">
              {errorMessage}
            </div>
          ) : isLoading && dmRows.length === 0 ? (
            <div className="flex min-h-64 flex-col items-center justify-center gap-2 bg-sky-100 px-4 py-8 text-sm text-slate-600">
              <p className="font-medium text-slate-800">Loading data models…</p>
              {loadProgress?.phase === "listing" ? (
                <p className="max-w-md text-center text-xs text-slate-500">
                  Listing data model definitions from CDF… {loadProgress.itemsLoaded} items fetched,{" "}
                  {loadProgress.uniqueModels} unique data models so far.
                </p>
              ) : null}
              {loadProgress?.phase === "details" ? (
                <p className="max-w-md text-center text-xs text-slate-500">
                  Loading data model details (inline views)… batch {loadProgress.batchIndex} of{" "}
                  {loadProgress.batchTotal}.
                </p>
              ) : null}
              {!loadProgress ? (
                <p className="text-xs text-slate-500">Preparing request…</p>
              ) : null}
            </div>
          ) : dmRows.length === 0 || versions.length === 0 ? (
            <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
              No data models or versions found.
            </div>
          ) : filteredDmRows.length === 0 ? (
            <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
              {selectedModelKey ? "No row for this data model." : "No data models or versions found."}
            </div>
          ) : filteredVersions.length === 0 ? (
            <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
              No version columns for the current filter.
            </div>
          ) : gridVersions.length === 0 && hasChecksumVersions ? (
            <div className="flex flex-col items-start gap-3 bg-sky-50 px-4 py-6 text-sm text-slate-700">
              <p>{t("dataCatalog.versionMatrix.onlyChecksumColumns")}</p>
              <label className="flex cursor-pointer items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={showChecksumVersions}
                  onChange={(e) => setShowChecksumVersions(e.target.checked)}
                  className="rounded border-slate-300"
                />
                <span>{t("dataCatalog.versionMatrix.showChecksumVersions")}</span>
              </label>
            </div>
          ) : (
            <>
              {hasChecksumVersions ? (
                <div className="border-b border-slate-200 bg-white px-3 py-2">
                  <label className="flex cursor-pointer items-center gap-2 text-xs text-slate-700">
                    <input
                      type="checkbox"
                      checked={showChecksumVersions}
                      onChange={(e) => setShowChecksumVersions(e.target.checked)}
                      className="rounded border-slate-300"
                    />
                    <span>{t("dataCatalog.versionMatrix.showChecksumVersions")}</span>
                  </label>
                </div>
              ) : null}
              {visibleDmLegendIds.size > 0 ? (
                <div className="bg-sky-50 px-3 py-2 text-xs text-slate-600">
                  <p className="mb-2 text-[11px] text-slate-500">
                    Click a legend entry to filter rows. Click again to clear.
                  </p>
                  <div className="flex flex-wrap items-center gap-2">
                    {DATA_MODEL_VERSION_LEGEND_ENTRIES.filter((e) => visibleDmLegendIds.has(e.id)).map(
                      ({ id, swatch, label }) => {
                        const active = dmLegendFilter === id;
                        return (
                          <button
                            key={id}
                            type="button"
                            onClick={() => setDmLegendFilter((prev) => (prev === id ? null : id))}
                            className={`flex max-w-[min(100%,20rem)] cursor-pointer items-center gap-2 rounded-md border-0 py-1 pl-1.5 pr-2 text-left transition ${
                              active
                                ? "bg-white/95 text-slate-900 shadow-sm ring-2 ring-slate-800 ring-offset-1 ring-offset-sky-50"
                                : "bg-transparent text-slate-600 hover:bg-white/70"
                            }`}
                          >
                            {swatch}
                            <span>{label}</span>
                          </button>
                        );
                      }
                    )}
                  </div>
                </div>
              ) : null}
              {legendFilteredDmRows.length === 0 && dmLegendFilter ? (
                <div className="flex min-h-48 items-center justify-center bg-sky-100 px-4 text-center text-sm text-slate-600">
                  No rows match this legend filter. Click the legend entry again to clear.
                </div>
              ) : (
                <VersioningGridScroll
                  contentWidth={dmGridContentWidth}
                  svgWidth={dmGridContentWidth}
                  bodySvgHeight={dmGridSvgHeight}
                  headerSvgRef={headerSvgRef}
                  bodySvgRef={bodySvgRef}
                />
              )}
            </>
          )}
        </div>
        <div className="flex min-h-0 w-64 shrink-0 flex-col rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
          {pinnedBubble ? (
            <>
              <div className="flex shrink-0 items-center justify-between gap-2">
                <span className="font-semibold">
                  {pinnedBubble.label} {pinnedBubble.version}
                </span>
                <button
                  type="button"
                  className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-slate-500 hover:bg-slate-200"
                  onClick={() => setPinnedBubble(null)}
                >
                  Unpin
                </button>
              </div>
              {(() => {
                const row = dmRows.find((r) => r.key === pinnedBubble.dmKey);
                if (!row || row.versions.size < 2) return null;
                return (
                  <button
                    type="button"
                    className="mt-2 w-full rounded-md border border-slate-200 bg-white px-2 py-1.5 text-left text-[11px] font-medium text-slate-700 hover:bg-slate-100"
                    onClick={() => setVersionHistoryDmKey(row.key)}
                  >
                    {t("dataCatalog.versionHistory.openPinned")}
                  </button>
                );
              })()}
              <p className="mt-1 shrink-0 text-slate-500">Referrers</p>
              <ul className="mt-1 min-h-0 flex-1 space-y-1 overflow-auto pl-1">
                {referrers.length === 0 ? (
                  <li className="text-slate-500">No referrers found.</li>
                ) : (
                  referrers.map((item, i) => (
                    <li key={i} className="flex items-center gap-1.5">
                      {item.type === "dataModel" ? (
                        <span className="text-slate-500">Data model</span>
                      ) : (
                        <span className="text-slate-500">Transformation</span>
                      )}
                      {item.url ? (
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noreferrer"
                          className="truncate text-blue-600 hover:underline"
                        >
                          {item.type === "dataModel" ? item.label : item.name}
                        </a>
                      ) : (
                        <span className="truncate">
                          {item.type === "dataModel" ? item.label : item.name}
                        </span>
                      )}
                    </li>
                  ))
                )}
              </ul>
            </>
          ) : (
            <div className="flex flex-1 items-center text-slate-500">Click a bubble to see referrers.</div>
          )}
        </div>
      </div>
    </section>
  );
}
