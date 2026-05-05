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
import { cachedTransformationsList } from "@/transformations/transformations-cache";
import {
  cachedDataModelsList,
  cachedViewsList,
  cachedViewsRetrieve,
} from "@/shared/dms-catalog-cache";
import {
  getDataModelUrl,
  getDataModelViewPreviewUrl,
  getTransformationPreviewUrl,
} from "@/shared/cdf-browser-url";
import { formatResourceDisplayLabel } from "@/shared/format-resource-display-label";
import { useI18n } from "@/shared/i18n";
import {
  compareVersionStrings,
  countImplicitViewVersions,
  cycleLegendFilterState,
  isChecksumLikeVersion,
  type LegendFilterState,
} from "./versioning-utils";
import { GRID_VERSION_HEADER_HEIGHT, VersioningGridScroll } from "./VersioningGridScroll";

const ROW_HEIGHT = 36;
const COL_WIDTH = 56;
const LABEL_WIDTH = 200;
/** Opaque auto-generated view versions; shown as count only, never as matrix columns. */
const IMPLICIT_COL_WIDTH = 44;
const VIEW_VERSION_AREA_START = LABEL_WIDTH + IMPLICIT_COL_WIDTH;
const PADDING = 20;
const SMALL_R = 3;
const LARGE_R = 6;

type ViewVersionItem = {
  space: string;
  externalId: string;
  version: string;
  name?: string;
  createdTime?: number;
  lastUpdatedTime?: number;
  properties?: Record<string, unknown>;
  filter?: unknown;
  implements?: unknown[];
};

type ViewRow = {
  key: string;
  label: string;
  versions: Map<string, ViewVersionItem>;
};

function viewRowSearchHaystack(row: ViewRow, detailsMap: Map<string, ViewVersionItem>): string {
  const parts: string[] = [row.label, row.key];
  const colon = row.key.indexOf(":");
  if (colon >= 0) {
    parts.push(row.key.slice(0, colon), row.key.slice(colon + 1));
  }
  for (const ver of row.versions.keys()) {
    parts.push(ver);
    const detail = detailsMap.get(`${row.key}:${ver}`) ?? row.versions.get(ver);
    if (!detail) continue;
    if (typeof detail.name === "string" && detail.name.trim()) parts.push(detail.name.trim());
    const props = detail.properties;
    if (props && typeof props === "object") {
      for (const k of Object.keys(props)) parts.push(k);
    }
    const imp = detail.implements;
    if (Array.isArray(imp)) {
      for (const x of imp) {
        if (x && typeof x === "object" && "space" in x && "externalId" in x) {
          const o = x as { space: string; externalId: string; version?: string };
          parts.push(`${o.space}:${o.externalId}`, o.externalId, o.space);
          if (typeof o.version === "string" && o.version.trim()) parts.push(o.version.trim());
        }
      }
    }
  }
  return parts.join(" ").toLowerCase();
}

type CellDot = {
  x: number;
  y: number;
  r: number;
  fill: string;
  viewKey?: string;
  version?: string;
  label?: string;
  space?: string;
  externalId?: string;
  /** Write destination: "green" = latest column (indigo ring), red = older version in destination.view */
  borderTone?: "green" | "red" | null;
} | null;

type ReferrerItem =
  | { type: "note"; text: string }
  | { type: "view"; label: string; url?: string }
  | { type: "transformation"; id: string; name: string; url?: string }
  | { type: "dataModel"; key: string; baseKey: string; label: string; space: string; externalId: string; version?: string; url?: string };

function viewDefinitionFingerprint(v: {
  properties?: Record<string, unknown>;
  filter?: unknown;
  implements?: unknown[];
}): string {
  const p = v.properties ?? {};
  const keys = Object.keys(p).sort();
  const serialized: Record<string, unknown> = {};
  for (const k of keys) {
    serialized[k] = p[k];
  }
  return JSON.stringify({ properties: serialized, filter: v.filter ?? null, implements: v.implements ?? [] });
}

/** Cell suffix when transformation `destination.view` omits version (treated as latest column). */
const DEST_VIEW_VERSION_UNSPECIFIED = "__dest_view_latest__";

/** Legend entries double as row filters (views that have ≥1 matching cell). */
type ViewGridLegendFilterId =
  | "sizeSmall"
  | "sizeLarge"
  | "latestInUse"
  | "latestNotInUse"
  | "olderNotInUse"
  | "otherInUse"
  | "txLatestBorder"
  | "txOlderBorder"
  | "implicitVersions";

function computeViewGridCell(
  row: ViewRow,
  ver: string,
  filteredVersions: string[],
  detailsMap: Map<string, ViewVersionItem>,
  viewKeysInDataModel: Set<string>,
  viewKeysInTransformation: Set<string>,
  txByCell: ReadonlyMap<string, unknown>
): {
  rVal: number;
  fill: string;
  borderTone: "green" | "red" | null;
  legendFlags: Set<ViewGridLegendFilterId>;
} | null {
  const item = row.versions.get(ver);
  if (!item) return null;

  const rowVersionsOrdered = filteredVersions.filter((v) => row.versions.has(v));
  const latestVersion = rowVersionsOrdered[rowVersionsOrdered.length - 1];
  const inDataModel = viewKeysInDataModel.has(row.key);
  const inTransformation = viewKeysInTransformation.has(row.key);

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
    const currFp = viewDefinitionFingerprint(details);
    const prevFp = viewDefinitionFingerprint(prevDetails);
    rVal = currFp === prevFp ? SMALL_R : LARGE_R;
  }

  const isLatestVersion = ver === latestVersion;
  const inUse = inDataModel || inTransformation;
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
  const destLatestKey = `${row.key}:${DEST_VIEW_VERSION_UNSPECIFIED}`;
  const txHasDestinationHere =
    txByCell.has(cellKey) || (isLatestVersion && txByCell.has(destLatestKey));
  const borderTone: "green" | "red" | null =
    txHasDestinationHere && isLatestVersion ? "green" : txHasDestinationHere && !isLatestVersion ? "red" : null;

  const legendFlags = new Set<ViewGridLegendFilterId>();
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

function viewRowLegendFlagUnion(
  row: ViewRow,
  filteredVersions: string[],
  detailsMap: Map<string, ViewVersionItem>,
  viewKeysInDataModel: Set<string>,
  viewKeysInTransformation: Set<string>,
  txByCell: ReadonlyMap<string, unknown>
): Set<ViewGridLegendFilterId> {
  const union = new Set<ViewGridLegendFilterId>();
  for (const ver of filteredVersions) {
    const cell = computeViewGridCell(
      row,
      ver,
      filteredVersions,
      detailsMap,
      viewKeysInDataModel,
      viewKeysInTransformation,
      txByCell
    );
    if (cell) for (const id of cell.legendFlags) union.add(id);
  }
  if (countImplicitViewVersions(row.versions.keys()) > 0) union.add("implicitVersions");
  return union;
}

const VIEW_VERSION_LEGEND_ENTRIES: Array<{
  id: ViewGridLegendFilterId;
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
    label: "Write destination: latest view version (indigo ring)",
  },
  {
    id: "txOlderBorder",
    swatch: (
      <span className="inline-flex h-2.5 w-2.5 shrink-0 rounded-full border-2 border-red-600 bg-transparent" />
    ),
    label: "Write destination: older view version (red ring)",
  },
  {
    id: "implicitVersions",
    swatch: (
      <span className="inline-flex min-w-[1.25rem] items-center justify-center rounded border border-amber-600 bg-amber-50 px-1 text-[10px] font-semibold text-amber-800">
        #
      </span>
    ),
    label: "Has implicit (auto-generated) view versions",
  },
];

const TAB_THRESHOLD = 10;

/** Ring for transformation write destination on latest column — indigo reads clearly on green/orange/pink fills */
const STROKE_TX_LATEST = "#4f46e5";

/** Selected / pinned bubble — distinct from indigo tx-destination ring */
const STROKE_PINNED = "#c2410c";

const INITIAL_VIEW_DISPLAY_CAP = 100;
const INITIAL_UNIQUE_VIEW_FETCH_CAP = 100;
/** If the catalog pauses after the initial cap but unique views are still below this, fetch until the list ends or this cap (avoids "Load all" for small catalogs). */
const AUTO_COMPLETE_UNIQUE_VIEW_CAP = INITIAL_UNIQUE_VIEW_FETCH_CAP * 2;
const VIEWS_LIST_PAGE_LIMIT = 250;
const VIEW_DETAILS_BATCH = 50;

function countUniqueViewKeys(items: ViewVersionItem[]): number {
  const s = new Set<string>();
  for (const i of items) s.add(`${i.space}:${i.externalId}`);
  return s.size;
}

function buildMatrixStateFromCatalog(listItems: ViewVersionItem[]): { rows: ViewRow[]; versions: string[] } {
  const viewMap = new Map<string, Map<string, ViewVersionItem>>();
  const versionSet = new Set<string>();

  for (const item of listItems) {
    const v = String(item.version ?? "latest");
    versionSet.add(v);
    const key = `${item.space}:${item.externalId}`;
    let versions = viewMap.get(key);
    if (!versions) {
      versions = new Map();
      viewMap.set(key, versions);
    }
    versions.set(v, item);
  }

  const allVersions = Array.from(versionSet);
  allVersions.sort((a, b) => {
    const cmp = compareVersionStrings(a, b);
    if (cmp !== 0) return cmp;
    const aItem = listItems.find((i) => String(i.version ?? "latest") === a);
    const bItem = listItems.find((i) => String(i.version ?? "latest") === b);
    const aTime = aItem?.createdTime ?? 0;
    const bTime = bItem?.createdTime ?? 0;
    if (aTime !== bTime) return aTime - bTime;
    return String(a).localeCompare(String(b));
  });

  const rows: ViewRow[] = [];
  for (const [viewKey, verMap] of viewMap) {
    const first = Array.from(verMap.values())[0];
    const label = formatResourceDisplayLabel(first?.name, first?.externalId, viewKey);
    rows.push({
      key: viewKey,
      label,
      versions: verMap,
    });
  }
  rows.sort((a, b) => a.label.localeCompare(b.label));
  return { rows, versions: allVersions };
}

type ViewVersionsFullCatalogSnapshot = {
  project: string;
  listItems: ViewVersionItem[];
  detailsEntries: Array<[string, ViewVersionItem]>;
};

let viewVersionsFullCatalogSnapshot: ViewVersionsFullCatalogSnapshot | null = null;

type TransformationDestinationView = {
  space?: string;
  externalId?: string;
  version?: string;
};

type TransformationApiItem = {
  id: number | string;
  name?: string;
  query?: string;
  destination?: { view?: TransformationDestinationView };
};

type ViewCellPinIndex = {
  txByCell: Map<string, Array<{ id: string; name: string }>>;
  dmByCell: Map<
    string,
    Array<{ key: string; baseKey: string; label: string; space: string; externalId: string; version: string }>
  >;
};

type DataModelOption = { key: string; baseKey: string; label: string; viewKeys: Set<string> };

function dataModelRefFromOption(opt: { key: string; baseKey: string }): {
  space: string;
  externalId: string;
  version: string;
} {
  const prefix = `${opt.baseKey}:`;
  const version = opt.key.startsWith(prefix) ? opt.key.slice(prefix.length) : "latest";
  const colon = opt.baseKey.indexOf(":");
  const space = colon >= 0 ? opt.baseKey.slice(0, colon) : "";
  const externalId = colon >= 0 ? opt.baseKey.slice(colon + 1) : opt.baseKey;
  return { space, externalId, version: version || "latest" };
}

function modelsContainingView(viewKey: string, options: DataModelOption[]): DataModelOption[] {
  return options.filter((o) => o.viewKeys.has(viewKey)).sort((a, b) => a.label.localeCompare(b.label));
}

function viewPreviewUrlForRow(
  project: string,
  viewKey: string,
  viewExternalId: string,
  options: DataModelOption[]
): { url?: string; models: DataModelOption[] } {
  const models = modelsContainingView(viewKey, options);
  const first = models[0];
  if (!first) return { models, url: undefined };
  const dm = dataModelRefFromOption(first);
  return {
    models,
    url: getDataModelViewPreviewUrl(
      project,
      dm.space,
      dm.externalId,
      dm.version,
      viewExternalId
    ),
  };
}

export function ViewVersions() {
  const { t } = useI18n();
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { dataModels, dataModelsStatus, loadDataModels, retrieveDataModels } = useAppData();
  const [status, setStatus] = useState<"idle" | "loading" | "success" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loadProgress, setLoadProgress] = useState<
    | { phase: "listing"; itemsLoaded: number; uniqueViews: number }
    | { phase: "details"; batchIndex: number; batchTotal: number }
    | null
  >(null);
  const [resumeViewsListCursor, setResumeViewsListCursor] = useState<string | undefined>(undefined);
  const [showAllViewRows, setShowAllViewRows] = useState(false);
  const catalogListItemsRef = useRef<ViewVersionItem[]>([]);
  const resumeViewsListCursorRef = useRef<string | undefined>(undefined);
  const detailsMapRef = useRef<Map<string, ViewVersionItem>>(new Map());
  const [viewRows, setViewRows] = useState<ViewRow[]>([]);
  const [versions, setVersions] = useState<string[]>([]);
  const [detailsMap, setDetailsMap] = useState<Map<string, ViewVersionItem>>(new Map());
  const [modelOptions, setModelOptions] = useState<DataModelOption[]>([]);
  const [selectedModelKey, setSelectedModelKey] = useState<string | null>(null);
  const [viewKeysInTransformation, setViewKeysInTransformation] = useState<Set<string>>(new Set());
  const [transformationRefsByModelVersion, setTransformationRefsByModelVersion] = useState<
    Map<string, Array<{ id: string; name: string }>>
  >(new Map());
  const [pinnedBubble, setPinnedBubble] = useState<
    | { type: "view"; viewKey: string; version: string; label: string; space: string; externalId: string }
    | {
        type: "implicitVersions";
        viewKey: string;
        label: string;
        space: string;
        externalId: string;
        versions: string[];
      }
    | { type: "model"; baseKey: string; version: string; label: string; space: string; externalId: string }
    | null
  >(null);
  const [modelVersionRefs, setModelVersionRefs] = useState<
    Map<string, { versions: Set<string>; hasUnspecified: boolean }>
  >(new Map());
  const [viewCellPinIndex, setViewCellPinIndex] = useState<ViewCellPinIndex>({
    txByCell: new Map(),
    dmByCell: new Map(),
  });
  const [viewLegendFilter, setViewLegendFilter] =
    useState<LegendFilterState<ViewGridLegendFilterId>>(null);
  const [matrixSearch, setMatrixSearch] = useState("");
  const [selectedModelVersions, setSelectedModelVersions] = useState<
    Array<{ version: string; createdTime?: number }>
  >([]);
  const modelVersionsSvgRef = useRef<SVGSVGElement | null>(null);
  const modelVersionsScrollRef = useRef<HTMLDivElement | null>(null);
  const modelVersionsTopRailRef = useRef<HTMLDivElement | null>(null);
  const modelVersionsBottomRailRef = useRef<HTMLDivElement | null>(null);
  const modelVersionsSyncing = useRef(false);
  const headerSvgRef = useRef<SVGSVGElement | null>(null);
  const bodySvgRef = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    if (!isSdkLoading) loadDataModels();
  }, [isSdkLoading, loadDataModels]);

  useEffect(() => {
    resumeViewsListCursorRef.current = resumeViewsListCursor;
  }, [resumeViewsListCursor]);

  useEffect(() => {
    detailsMapRef.current = detailsMap;
  }, [detailsMap]);

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
        const label = formatResourceDisplayLabel(model.name, model.externalId, key);

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
    if (isSdkLoading || modelOptions.length === 0) return;
    let cancelled = false;
    const load = async () => {
      const destinationTxByCell = new Map<string, Array<{ id: string; name: string }>>();
      const pushDestTx = (cellKey: string, txItem: { id: string; name: string }) => {
        let arr = destinationTxByCell.get(cellKey);
        if (!arr) {
          arr = [];
          destinationTxByCell.set(cellKey, arr);
        }
        if (!arr.some((x) => x.id === txItem.id)) arr.push(txItem);
      };

      try {
        const response = (await cachedTransformationsList(sdk, {
          includePublic: "true",
          limit: "1000",
        })) as { data?: { items?: TransformationApiItem[] } };
        const items = response.data?.items ?? [];
        const idsNeedingDetail: string[] = [];
        for (const t of items) {
          let destination = t.destination;
          let query = t.query ?? "";
          const needDetail =
            !destination?.view?.space ||
            !destination?.view?.externalId ||
            !String(query).trim();
          if (needDetail) idsNeedingDetail.push(String(t.id));
        }
        const detailById = await fetchTransformationsByIds(sdk, sdk.project, idsNeedingDetail);

        const modelRefs = new Set<string>();
        const versionRefs = new Map<string, { versions: Set<string>; hasUnspecified: boolean }>();
        const txByModelVersion = new Map<string, Array<{ id: string; name: string }>>();
        const destinationViewKeys = new Set<string>();

        for (const t of items) {
          if (cancelled) return;
          const txItem = { id: String(t.id), name: t.name ?? String(t.id) };

          let destination = t.destination;
          let query = t.query ?? "";
          const detail = detailById.get(String(t.id));
          if (detail) {
            if (detail.destination?.view?.space && detail.destination?.view?.externalId) {
              destination = detail.destination;
            }
            if (detail.query != null && String(detail.query).trim() !== "") {
              query = detail.query;
            }
          }

          const dv = destination?.view;
          if (dv?.space && dv?.externalId) {
            const vk = `${dv.space}:${dv.externalId}`;
            destinationViewKeys.add(vk);
            const verPart = dv.version?.trim();
            const versionToken = verPart ? verPart : DEST_VIEW_VERSION_UNSPECIFIED;
            pushDestTx(`${vk}:${versionToken}`, txItem);
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
          setViewCellPinIndex({ txByCell: destinationTxByCell, dmByCell: new Map() });
        }
        const viewKeys = new Set<string>();
        for (const opt of modelOptions) {
          if (modelRefs.has(opt.baseKey)) {
            for (const vk of opt.viewKeys) viewKeys.add(vk);
          }
        }
        for (const vk of destinationViewKeys) viewKeys.add(vk);
        if (!cancelled) setViewKeysInTransformation(viewKeys);
      } catch {
        if (!cancelled) {
          setViewKeysInTransformation(new Set());
          setViewCellPinIndex({ txByCell: new Map(), dmByCell: new Map() });
        }
      }
    };
    load();
    return () => { cancelled = true; };
  }, [sdk, isSdkLoading, modelOptions]);

  const viewKeysInDataModel = useMemo(() => {
    const set = new Set<string>();
    for (const opt of modelOptions) {
      for (const vk of opt.viewKeys) set.add(vk);
    }
    return set;
  }, [modelOptions]);

  useEffect(() => {
    if (!selectedModelKey) {
      setSelectedModelVersions([]);
      return;
    }
    const opt = modelOptions.find((o) => o.key === selectedModelKey);
    if (!opt) return;
    let cancelled = false;
    const load = async () => {
      try {
        const items: Array<{ version: string; createdTime?: number }> = [];
        let cursor: string | undefined;
        do {
          const response = (await cachedDataModelsList(sdk, {
            includeGlobal: true,
            allVersions: true,
            limit: 250,
            cursor,
          })) as {
            items?: Array<{ space: string; externalId: string; version?: string; createdTime?: number }>;
            nextCursor?: string;
          };
          const listItems = (response.items ?? []) as Array<{ space: string; externalId: string; version?: string; createdTime?: number }>;
          for (const m of listItems) {
            const baseKey = `${m.space}:${m.externalId}`;
            if (baseKey === opt.baseKey) {
              items.push({
                version: m.version ?? "latest",
                createdTime: m.createdTime,
              });
            }
          }
          cursor = response.nextCursor ?? undefined;
        } while (cursor);

        items.sort((a, b) => {
          const cmp = compareVersionStrings(String(a.version ?? ""), String(b.version ?? ""));
          if (cmp !== 0) return cmp;
          const at = a.createdTime ?? 0;
          const bt = b.createdTime ?? 0;
          if (at !== bt) return at - bt;
          return String(a.version).localeCompare(String(b.version));
        });
        if (!cancelled) setSelectedModelVersions(items);
      } catch {
        if (!cancelled) setSelectedModelVersions([]);
      }
    };
    load();
    return () => { cancelled = true; };
  }, [selectedModelKey, modelOptions, sdk]);

  useEffect(() => {
    setPinnedBubble(null);
  }, [selectedModelKey]);

  useEffect(() => {
    setViewLegendFilter(null);
  }, [selectedModelKey]);

  const modelFilteredViewRows = useMemo(() => {
    if (!selectedModelKey) return viewRows;
    const opt = modelOptions.find((o) => o.key === selectedModelKey);
    if (!opt || opt.viewKeys.size === 0) return viewRows;
    return viewRows.filter((row) => opt.viewKeys.has(row.key));
  }, [viewRows, selectedModelKey, modelOptions]);

  const filteredViewRows = useMemo(() => {
    const q = matrixSearch.trim().toLowerCase();
    if (!q) return modelFilteredViewRows;
    return modelFilteredViewRows.filter((row) => viewRowSearchHaystack(row, detailsMap).includes(q));
  }, [modelFilteredViewRows, matrixSearch, detailsMap]);

  const filteredVersions = useMemo(() => {
    if (filteredViewRows.length === 0) return versions;
    const used = new Set<string>();
    for (const row of filteredViewRows) {
      for (const v of row.versions.keys()) used.add(v);
    }
    return versions.filter((v) => used.has(v));
  }, [versions, filteredViewRows]);

  const matrixVersions = useMemo(
    () => filteredVersions.filter((v) => !isChecksumLikeVersion(v)),
    [filteredVersions]
  );

  const viewRowLegendFlagsByKey = useMemo(() => {
    const m = new Map<string, Set<ViewGridLegendFilterId>>();
    const txByCell = viewCellPinIndex.txByCell;
    for (const row of filteredViewRows) {
      m.set(
        row.key,
        viewRowLegendFlagUnion(
          row,
          matrixVersions,
          detailsMap,
          viewKeysInDataModel,
          viewKeysInTransformation,
          txByCell
        )
      );
    }
    return m;
  }, [
    filteredViewRows,
    matrixVersions,
    detailsMap,
    viewKeysInDataModel,
    viewKeysInTransformation,
    viewCellPinIndex.txByCell,
  ]);

  const legendFilteredViewRows = useMemo(() => {
    if (!viewLegendFilter) return filteredViewRows;
    const { id, mode } = viewLegendFilter;
    return filteredViewRows.filter((row) => {
      const has = viewRowLegendFlagsByKey.get(row.key)?.has(id) ?? false;
      return mode === "include" ? has : !has;
    });
  }, [filteredViewRows, viewLegendFilter, viewRowLegendFlagsByKey]);

  const cappedLegendViewRows = useMemo(() => {
    const searchActive = matrixSearch.trim().length > 0;
    if (
      showAllViewRows ||
      searchActive ||
      legendFilteredViewRows.length <= INITIAL_VIEW_DISPLAY_CAP
    ) {
      return legendFilteredViewRows;
    }
    return legendFilteredViewRows.slice(0, INITIAL_VIEW_DISPLAY_CAP);
  }, [legendFilteredViewRows, showAllViewRows, matrixSearch]);

  const versionsUsedByCappedRows = useMemo(() => {
    const used = new Set<string>();
    for (const row of cappedLegendViewRows) {
      for (const v of row.versions.keys()) used.add(v);
    }
    return versions.filter((v) => used.has(v));
  }, [versions, cappedLegendViewRows]);

  const gridVersions = useMemo(
    () => versionsUsedByCappedRows.filter((v) => !isChecksumLikeVersion(v)),
    [versionsUsedByCappedRows]
  );

  const usedViewLegendIds = useMemo(() => {
    const u = new Set<ViewGridLegendFilterId>();
    for (const row of filteredViewRows) {
      const flags = viewRowLegendFlagsByKey.get(row.key);
      if (flags) for (const id of flags) u.add(id);
    }
    return u;
  }, [filteredViewRows, viewRowLegendFlagsByKey]);

  const visibleViewLegendIds = useMemo(() => {
    const u = new Set<ViewGridLegendFilterId>();
    const showSizePair = usedViewLegendIds.has("sizeSmall") && usedViewLegendIds.has("sizeLarge");
    if (showSizePair) {
      if (usedViewLegendIds.has("sizeSmall")) u.add("sizeSmall");
      if (usedViewLegendIds.has("sizeLarge")) u.add("sizeLarge");
    }
    for (const id of usedViewLegendIds) {
      if (id === "sizeSmall" || id === "sizeLarge") continue;
      u.add(id);
    }
    return u;
  }, [usedViewLegendIds]);

  useEffect(() => {
    if (viewLegendFilter != null && !visibleViewLegendIds.has(viewLegendFilter.id)) {
      setViewLegendFilter(null);
    }
  }, [viewLegendFilter, visibleViewLegendIds]);

  const loadData = useCallback(
    async (extendToFull: boolean) => {
      if (isSdkLoading) return;
      setStatus("loading");
      setErrorMessage(null);
      setLoadProgress({ phase: "listing", itemsLoaded: 0, uniqueViews: 0 });
      try {
        const listItems: ViewVersionItem[] = extendToFull ? [...catalogListItemsRef.current] : [];
        let cursor: string | undefined = extendToFull ? resumeViewsListCursorRef.current : undefined;

        const skipListFetch = extendToFull && listItems.length > 0 && cursor == null;
        if (!skipListFetch) {
          let stoppedAfterInitialCap = false;
          do {
            const response = (await cachedViewsList(sdk, {
              includeGlobal: true,
              allVersions: true,
              limit: VIEWS_LIST_PAGE_LIMIT,
              cursor,
            })) as { items?: ViewVersionItem[]; nextCursor?: string };
            const items = (response.items ?? []) as ViewVersionItem[];
            listItems.push(...items);
            cursor = response.nextCursor ?? undefined;
            setLoadProgress({
              phase: "listing",
              itemsLoaded: listItems.length,
              uniqueViews: countUniqueViewKeys(listItems),
            });
            if (!extendToFull && cursor && countUniqueViewKeys(listItems) >= INITIAL_UNIQUE_VIEW_FETCH_CAP) {
              stoppedAfterInitialCap = true;
              break;
            }
          } while (cursor);

          if (!extendToFull && stoppedAfterInitialCap && cursor) {
            while (cursor && countUniqueViewKeys(listItems) < AUTO_COMPLETE_UNIQUE_VIEW_CAP) {
              const response = (await cachedViewsList(sdk, {
                includeGlobal: true,
                allVersions: true,
                limit: VIEWS_LIST_PAGE_LIMIT,
                cursor,
              })) as { items?: ViewVersionItem[]; nextCursor?: string };
              const items = (response.items ?? []) as ViewVersionItem[];
              listItems.push(...items);
              cursor = response.nextCursor ?? undefined;
              setLoadProgress({
                phase: "listing",
                itemsLoaded: listItems.length,
                uniqueViews: countUniqueViewKeys(listItems),
              });
            }
          }
        } else {
          setLoadProgress({
            phase: "listing",
            itemsLoaded: listItems.length,
            uniqueViews: countUniqueViewKeys(listItems),
          });
        }

        if (!cursor) {
          setResumeViewsListCursor(undefined);
        } else if (!skipListFetch) {
          setResumeViewsListCursor(cursor);
        }

        catalogListItemsRef.current = listItems;

        const refs = listItems
          .filter((i) => i.version != null && i.version !== "")
          .map((i) => ({
            space: i.space,
            externalId: i.externalId,
            version: String(i.version),
          }));

        const details = extendToFull ? new Map(detailsMapRef.current) : new Map<string, ViewVersionItem>();
        const batchTotal = Math.max(1, Math.ceil(refs.length / VIEW_DETAILS_BATCH));
        if (!extendToFull) {
          for (let i = 0; i < refs.length; i += VIEW_DETAILS_BATCH) {
            const batchIndex = Math.floor(i / VIEW_DETAILS_BATCH) + 1;
            setLoadProgress({ phase: "details", batchIndex, batchTotal });
            const batch = refs.slice(i, i + VIEW_DETAILS_BATCH);
            const response = (await cachedViewsRetrieve(
              sdk,
              batch as Array<Record<string, unknown>>,
              { includeInheritedProperties: false }
            )) as { items?: ViewVersionItem[] };
            for (const item of response.items ?? []) {
              const v = String(item.version ?? "latest");
              const key = `${item.space}:${item.externalId}:${v}`;
              details.set(key, item);
            }
          }
        } else {
          const missing = refs.filter((r) => {
            const k = `${r.space}:${r.externalId}:${r.version}`;
            return !details.has(k);
          });
          for (let i = 0; i < missing.length; i += VIEW_DETAILS_BATCH) {
            const batchIndex = Math.floor(i / VIEW_DETAILS_BATCH) + 1;
            const batchCount = Math.max(1, Math.ceil(missing.length / VIEW_DETAILS_BATCH));
            setLoadProgress({ phase: "details", batchIndex, batchTotal: batchCount });
            const batch = missing.slice(i, i + VIEW_DETAILS_BATCH);
            const response = (await cachedViewsRetrieve(
              sdk,
              batch as Array<Record<string, unknown>>,
              { includeInheritedProperties: false }
            )) as { items?: ViewVersionItem[] };
            for (const item of response.items ?? []) {
              const v = String(item.version ?? "latest");
              const key = `${item.space}:${item.externalId}:${v}`;
              details.set(key, item);
            }
          }
        }

        const { rows, versions: versionLabels } = buildMatrixStateFromCatalog(listItems);

        setViewRows(rows);
        setVersions(versionLabels);
        setDetailsMap(details);
        setLoadProgress(null);
        if (!cursor) {
          viewVersionsFullCatalogSnapshot = {
            project: sdk.project,
            listItems: listItems.map((i) => ({ ...i })),
            detailsEntries: [...details.entries()].map(([k, v]) => [k, { ...v }] as [string, ViewVersionItem]),
          };
        } else {
          viewVersionsFullCatalogSnapshot = null;
        }
        setStatus("success");
      } catch (error) {
        setLoadProgress(null);
        viewVersionsFullCatalogSnapshot = null;
        setErrorMessage(error instanceof Error ? error.message : "Failed to load views.");
        setStatus("error");
      }
    },
    [sdk, isSdkLoading]
  );

  useEffect(() => {
    if (isSdkLoading) return;

    if (viewVersionsFullCatalogSnapshot && viewVersionsFullCatalogSnapshot.project !== sdk.project) {
      viewVersionsFullCatalogSnapshot = null;
    }

    if (
      viewVersionsFullCatalogSnapshot &&
      viewVersionsFullCatalogSnapshot.project === sdk.project
    ) {
      const { listItems, detailsEntries } = viewVersionsFullCatalogSnapshot;
      catalogListItemsRef.current = listItems;
      const details = new Map<string, ViewVersionItem>(detailsEntries);
      detailsMapRef.current = details;
      setShowAllViewRows(false);
      setResumeViewsListCursor(undefined);
      const { rows, versions: versionLabels } = buildMatrixStateFromCatalog(listItems);
      setViewRows(rows);
      setVersions(versionLabels);
      setDetailsMap(details);
      setStatus("success");
      setLoadProgress(null);
      setErrorMessage(null);
      return;
    }

    setShowAllViewRows(false);
    setResumeViewsListCursor(undefined);
    catalogListItemsRef.current = [];
    void loadData(false);
  }, [isSdkLoading, loadData, sdk.project]);

  const { rows, linePath } = useMemo(() => {
    if (cappedLegendViewRows.length === 0) {
      return { rows: [], linePath: line<{ x: number; y: number }>() };
    }

    const lineGen = line<{ x: number; y: number }>()
      .x((d) => d.x)
      .y((d) => d.y);

    const rows: Array<{
      label: string;
      viewKey: string;
      space: string;
      externalId: string;
      implicitCount: number;
      implicitVersionStrings: string[];
      dots: CellDot[];
      connected: Array<{ x: number; y: number }>;
    }> = [];
    const txByCell = viewCellPinIndex.txByCell;

    for (let r = 0; r < cappedLegendViewRows.length; r++) {
      const row = cappedLegendViewRows[r];
      const implicitCount = countImplicitViewVersions(row.versions.keys());
      const implicitVersionStrings: string[] = [];
      for (const v of row.versions.keys()) {
        if (isChecksumLikeVersion(v)) implicitVersionStrings.push(v);
      }
      implicitVersionStrings.sort(compareVersionStrings);
      const colonIdx = row.key.indexOf(":");
      const space = colonIdx >= 0 ? row.key.slice(0, colonIdx) : "";
      const externalId = colonIdx >= 0 ? row.key.slice(colonIdx + 1) : row.key;
      const cy = PADDING + r * ROW_HEIGHT + ROW_HEIGHT / 2;
      const dots: CellDot[] = [];
      const connected: Array<{ x: number; y: number }> = [];

      for (let c = 0; c < gridVersions.length; c++) {
        const ver = gridVersions[c];
        const cx = VIEW_VERSION_AREA_START + PADDING + c * COL_WIDTH + COL_WIDTH / 2;

        const cell = computeViewGridCell(
          row,
          ver,
          gridVersions,
          detailsMap,
          viewKeysInDataModel,
          viewKeysInTransformation,
          txByCell
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
          viewKey: row.key,
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
        viewKey: row.key,
        space,
        externalId,
        implicitCount,
        implicitVersionStrings,
        dots,
        connected,
      });
    }

    return { rows, linePath: lineGen };
  }, [
    cappedLegendViewRows,
    gridVersions,
    detailsMap,
    viewKeysInDataModel,
    viewKeysInTransformation,
    viewCellPinIndex.txByCell,
  ]);

  const referrers = useMemo((): ReferrerItem[] => {
    if (!pinnedBubble) return [];
    const items: ReferrerItem[] = [];
    if (pinnedBubble.type === "implicitVersions") {
      const { url: dmPreviewUrl, models } = viewPreviewUrlForRow(
        sdk.project,
        pinnedBubble.viewKey,
        pinnedBubble.externalId,
        modelOptions
      );
      if (models.length > 1) {
        items.push({
          type: "note",
          text: `This view appears on ${models.length} catalog data models. Fusion links use the first (${models[0].label}) as data model context—switch data model in Fusion if you need another.`,
        });
      } else if (models.length === 0) {
        items.push({
          type: "note",
          text:
            "No loaded catalog data model lists this view as an inline view, so a Data management preview URL cannot be built.",
        });
      }
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
      for (const ver of pinnedBubble.versions) {
        items.push({
          type: "view",
          label: `${pinnedBubble.label} · ${ver}`,
          url: dmPreviewUrl,
        });
        pushDestTxList(viewCellPinIndex.txByCell.get(`${pinnedBubble.viewKey}:${ver}`));
      }
      return items;
    }
    if (pinnedBubble.type === "view") {
      const { url: dmPreviewUrl, models } = viewPreviewUrlForRow(
        sdk.project,
        pinnedBubble.viewKey,
        pinnedBubble.externalId,
        modelOptions
      );
      if (models.length > 1) {
        items.push({
          type: "note",
          text: `This view appears on ${models.length} catalog data models. The Fusion link uses the first (${models[0].label}) as data model context—switch data model in Fusion if you need another.`,
        });
      } else if (models.length === 0) {
        items.push({
          type: "note",
          text:
            "No loaded catalog data model lists this view as an inline view, so a Data management preview URL cannot be built.",
        });
      }
      items.push({
        type: "view",
        label: `${pinnedBubble.label} ${pinnedBubble.version}`,
        url: dmPreviewUrl,
      });
      const row = viewRows.find((r) => r.key === pinnedBubble.viewKey);
      const orderedVers = versions.filter((v) => row?.versions.has(v) ?? false);
      const latestV = orderedVers[orderedVers.length - 1];
      const cellKey = `${pinnedBubble.viewKey}:${pinnedBubble.version}`;
      const destLatestKey = `${pinnedBubble.viewKey}:${DEST_VIEW_VERSION_UNSPECIFIED}`;
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
      pushDestTxList(viewCellPinIndex.txByCell.get(cellKey));
      if (latestV != null && pinnedBubble.version === latestV) {
        pushDestTxList(viewCellPinIndex.txByCell.get(destLatestKey));
      }
    } else {
      items.push({
        type: "dataModel",
        key: pinnedBubble.baseKey,
        baseKey: pinnedBubble.baseKey,
        label: pinnedBubble.label,
        space: pinnedBubble.space,
        externalId: pinnedBubble.externalId,
        version: pinnedBubble.version,
        url: getDataModelUrl(sdk.project, pinnedBubble.space, pinnedBubble.externalId, pinnedBubble.version),
      });
      const latestVer = selectedModelVersions[selectedModelVersions.length - 1]?.version;
      const isPinnedLatest = latestVer != null && pinnedBubble.version === latestVer;
      const seenTx = new Set<string>();
      const pushTxs = (list: Array<{ id: string; name: string }> | undefined) => {
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
      pushTxs(transformationRefsByModelVersion.get(`${pinnedBubble.baseKey}:${pinnedBubble.version}`));
      if (isPinnedLatest) {
        pushTxs(transformationRefsByModelVersion.get(`${pinnedBubble.baseKey}:`));
      }
    }
    return items;
  }, [
    pinnedBubble,
    viewCellPinIndex,
    transformationRefsByModelVersion,
    selectedModelVersions,
    viewRows,
    versions,
    sdk.project,
    modelOptions,
  ]);

  const handleViewBubbleClick = useCallback((d: NonNullable<CellDot>) => {
    if (d.viewKey != null && d.version != null && d.label != null && d.space != null && d.externalId != null) {
      setPinnedBubble({
        type: "view",
        viewKey: d.viewKey,
        version: d.version,
        label: d.label,
        space: d.space,
        externalId: d.externalId,
      });
    }
  }, []);

  const handleImplicitCountClick = useCallback(
    (d: {
      viewKey: string;
      label: string;
      space: string;
      externalId: string;
      implicitVersionStrings: string[];
    }) => {
      if (d.implicitVersionStrings.length === 0) return;
      setPinnedBubble({
        type: "implicitVersions",
        viewKey: d.viewKey,
        label: d.label,
        space: d.space,
        externalId: d.externalId,
        versions: d.implicitVersionStrings,
      });
    },
    []
  );

  const handleModelBubbleClick = useCallback((d: { baseKey: string; version: string; label: string; space: string; externalId: string }) => {
    setPinnedBubble({
      type: "model",
      baseKey: d.baseKey,
      version: d.version,
      label: d.label,
      space: d.space,
      externalId: d.externalId,
    });
  }, []);

  const scrollModelVersionsBy = useCallback((delta: number) => {
    modelVersionsScrollRef.current?.scrollBy({ left: delta, behavior: "smooth" });
  }, []);

  const onModelVersionsMainScroll = useCallback(() => {
    const m = modelVersionsScrollRef.current;
    if (!m || modelVersionsSyncing.current) return;
    modelVersionsSyncing.current = true;
    const sl = m.scrollLeft;
    if (modelVersionsTopRailRef.current) modelVersionsTopRailRef.current.scrollLeft = sl;
    if (modelVersionsBottomRailRef.current) modelVersionsBottomRailRef.current.scrollLeft = sl;
    requestAnimationFrame(() => {
      modelVersionsSyncing.current = false;
    });
  }, []);

  const onModelVersionsRailScroll = useCallback((source: "top" | "bottom") => {
    const m = modelVersionsScrollRef.current;
    const r =
      source === "top" ? modelVersionsTopRailRef.current : modelVersionsBottomRailRef.current;
    if (!m || !r || modelVersionsSyncing.current) return;
    modelVersionsSyncing.current = true;
    m.scrollLeft = r.scrollLeft;
    const other = source === "top" ? modelVersionsBottomRailRef.current : modelVersionsTopRailRef.current;
    if (other) other.scrollLeft = r.scrollLeft;
    requestAnimationFrame(() => {
      modelVersionsSyncing.current = false;
    });
  }, []);

  useEffect(() => {
    if (!headerSvgRef.current) return;
    const versionCount = gridVersions.length;
    const width = VIEW_VERSION_AREA_START + PADDING * 2 + versionCount * COL_WIDTH;
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
      .append("text")
      .attr("x", LABEL_WIDTH + IMPLICIT_COL_WIDTH / 2)
      .attr("y", PADDING - 4)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", "#1e293b")
      .text("Implicit");
    root
      .append("g")
      .selectAll("text.version")
      .data(gridVersions)
      .enter()
      .append("text")
      .attr("class", "version")
      .attr("x", (_, i) => VIEW_VERSION_AREA_START + PADDING + i * COL_WIDTH + COL_WIDTH / 2)
      .attr("y", PADDING - 4)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", "#1e293b")
      .text((d) => d);
  }, [gridVersions]);

  useEffect(() => {
    if (!bodySvgRef.current || rows.length === 0) return;
    const handler = handleViewBubbleClick;
    const implicitHandler = handleImplicitCountClick;
    const pinned = pinnedBubble?.type === "view" ? pinnedBubble : null;
    const pinnedImplicit =
      pinnedBubble?.type === "implicitVersions" ? pinnedBubble : null;
    const versionCount = gridVersions.length;
    const width = VIEW_VERSION_AREA_START + PADDING * 2 + versionCount * COL_WIDTH;
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
      .each(function (rowData) {
        const g = select(this);

        if (rowData.connected.length > 0) {
          g.append("path")
            .attr("d", linePath(rowData.connected) ?? "")
            .attr("fill", "none")
            .attr("stroke", "rgba(255,255,255,0.8)")
            .attr("stroke-width", 1);
        }

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
            if (pinned && d.viewKey === pinned.viewKey && d.version === pinned.version) {
              return STROKE_PINNED;
            }
            if (d.borderTone === "red") return "#dc2626";
            if (d.borderTone === "green") return STROKE_TX_LATEST;
            return "none";
          })
          .attr("stroke-width", (d) => {
            if (pinned && d.viewKey === pinned.viewKey && d.version === pinned.version) return 3;
            if (d.borderTone) return 2;
            return 0;
          })
          .style("cursor", "pointer")
          .on("click", (_ev, d) => {
            if (d.viewKey != null && d.version != null) handler(d);
          });
      });

    main
      .selectAll("text.label")
      .data(rows)
      .enter()
      .append("text")
      .attr("class", "label")
      .attr("x", LABEL_WIDTH - 8)
      .attr("y", (_, i) => PADDING + i * ROW_HEIGHT + ROW_HEIGHT / 2 + 4)
      .attr("text-anchor", "end")
      .attr("font-size", 11)
      .attr("fill", "#1e293b")
      .attr("overflow", "hidden")
      .attr("text-overflow", "ellipsis")
      .text((d) => d.label);

    main
      .selectAll("text.implicitCount")
      .data(rows)
      .enter()
      .append("text")
      .attr("class", "implicitCount")
      .attr("x", LABEL_WIDTH + IMPLICIT_COL_WIDTH / 2)
      .attr("y", (_, i) => PADDING + i * ROW_HEIGHT + ROW_HEIGHT / 2 + 4)
      .attr("text-anchor", "middle")
      .attr("font-size", 11)
      .attr("font-weight", (d) =>
        pinnedImplicit && d.viewKey === pinnedImplicit.viewKey ? "700" : "400"
      )
      .attr("text-decoration", (d) =>
        pinnedImplicit && d.viewKey === pinnedImplicit.viewKey ? "underline" : "none"
      )
      .attr("fill", (d) => {
        if (d.implicitCount <= 0) return "#94a3b8";
        if (pinnedImplicit && d.viewKey === pinnedImplicit.viewKey) return "#92400e";
        return "#b45309";
      })
      .style("cursor", (d) => (d.implicitCount > 0 ? "pointer" : "default"))
      .text((d) => String(d.implicitCount))
      .on("click", (_ev, d) => {
        if (d.implicitCount > 0) {
          implicitHandler({
            viewKey: d.viewKey,
            label: d.label,
            space: d.space,
            externalId: d.externalId,
            implicitVersionStrings: d.implicitVersionStrings,
          });
        }
      });
  }, [rows, gridVersions, linePath, handleViewBubbleClick, handleImplicitCountClick, pinnedBubble]);

  const modelVersionRow = useMemo(() => {
    if (!selectedModelKey || selectedModelVersions.length === 0) return null;
    const opt = modelOptions.find((o) => o.key === selectedModelKey);
    if (!opt) return null;
    const entry = modelVersionRefs.get(opt.baseKey);
    const refVersions = entry?.versions ?? new Set();
    const hasUnspecified = entry?.hasUnspecified ?? false;
    const latestVer = selectedModelVersions[selectedModelVersions.length - 1]?.version;
    const colonIdx = opt.baseKey.indexOf(":");
    const spaceVal = colonIdx >= 0 ? opt.baseKey.slice(0, colonIdx) : "";
    const externalIdVal = colonIdx >= 0 ? opt.baseKey.slice(colonIdx + 1) : opt.baseKey;

    const dots: Array<{
      x: number;
      y: number;
      r: number;
      fill: string;
      baseKey: string;
      version: string;
      label: string;
      space: string;
      externalId: string;
      borderTone: "green" | "red" | null;
    }> = [];
    const connected: Array<{ x: number; y: number }> = [];
    const cy = PADDING + ROW_HEIGHT / 2;

    for (let c = 0; c < selectedModelVersions.length; c++) {
      const ver = selectedModelVersions[c].version ?? "";
      const cx = LABEL_WIDTH + PADDING + c * COL_WIDTH + COL_WIDTH / 2;
      const isLatest = ver === latestVer;
      const inUse = refVersions.has(ver) || (hasUnspecified && isLatest);
      const explicitlyTargeted = refVersions.has(ver);
      const borderTone: "green" | "red" | null =
        isLatest && (explicitlyTargeted || hasUnspecified)
          ? "green"
          : !isLatest && explicitlyTargeted
            ? "red"
            : null;
      const fill =
        isLatest && inUse
          ? "#22c55e"
          : isLatest && !inUse
            ? "#ea580c"
            : !inUse
              ? "#ec4899"
              : "white";
      dots.push({
        x: cx,
        y: cy,
        r: LARGE_R,
        fill,
        baseKey: opt.baseKey,
        version: ver,
        label: opt.label,
        space: spaceVal,
        externalId: externalIdVal,
        borderTone,
      });
      connected.push({ x: cx, y: cy });
    }

    return { label: `${opt.label} (versions)`, dots, connected };
  }, [selectedModelKey, selectedModelVersions, modelOptions, modelVersionRefs]);

  useEffect(() => {
    if (!modelVersionsSvgRef.current || !modelVersionRow) return;
    const handler = handleModelBubbleClick;
    const pinned = pinnedBubble?.type === "model" ? pinnedBubble : null;
    const versionCount = selectedModelVersions.length;
    const width = LABEL_WIDTH + PADDING * 2 + versionCount * COL_WIDTH;
    const height = PADDING * 2 + ROW_HEIGHT;
    const root = select(modelVersionsSvgRef.current);
    root.selectAll("*").remove();
    root.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);

    root
      .append("rect")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", width)
      .attr("height", height)
      .attr("fill", "skyblue");

    const main = root.append("g");

    main
      .selectAll("text.version")
      .data(selectedModelVersions.map((v) => v.version))
      .enter()
      .append("text")
      .attr("class", "version")
      .attr("x", (_, i) => LABEL_WIDTH + PADDING + i * COL_WIDTH + COL_WIDTH / 2)
      .attr("y", PADDING - 4)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", "#1e293b")
      .text((d) => d);

    const lineGen = line<{ x: number; y: number }>()
      .x((d) => d.x)
      .y((d) => d.y);

    main
      .append("path")
      .attr("d", lineGen(modelVersionRow.connected) ?? "")
      .attr("fill", "none")
      .attr("stroke", "rgba(255,255,255,0.8)")
      .attr("stroke-width", 1);

    main
      .selectAll("circle")
      .data(modelVersionRow.dots)
      .enter()
      .append("circle")
      .attr("cx", (d) => d.x)
      .attr("cy", (d) => d.y)
      .attr("r", (d) => d.r)
      .attr("fill", (d) => d.fill)
      .attr("stroke", (d) => {
        if (pinned && d.baseKey === pinned.baseKey && d.version === pinned.version) {
          return STROKE_PINNED;
        }
        if (d.borderTone === "red") return "#dc2626";
        if (d.borderTone === "green") return STROKE_TX_LATEST;
        return "none";
      })
      .attr("stroke-width", (d) => {
        if (pinned && d.baseKey === pinned.baseKey && d.version === pinned.version) return 3;
        if (d.borderTone) return 2;
        return 0;
      })
      .style("cursor", "pointer")
      .on("click", (_ev, d) => handler(d));

    main
      .append("text")
      .attr("class", "label")
      .attr("x", LABEL_WIDTH - 8)
      .attr("y", PADDING + ROW_HEIGHT / 2 + 4)
      .attr("text-anchor", "end")
      .attr("font-size", 11)
      .attr("fill", "#1e293b")
      .text(modelVersionRow.label);
  }, [modelVersionRow, selectedModelVersions, handleModelBubbleClick, pinnedBubble]);

  const isLoading = isSdkLoading || status === "loading";

  const hiddenRowCountByCap =
    !showAllViewRows &&
    matrixSearch.trim().length === 0 &&
    legendFilteredViewRows.length > INITIAL_VIEW_DISPLAY_CAP
      ? legendFilteredViewRows.length - INITIAL_VIEW_DISPLAY_CAP
      : 0;
  const hasMoreCatalogOnServer = Boolean(resumeViewsListCursor);

  const viewsContentWidth = VIEW_VERSION_AREA_START + PADDING * 2 + gridVersions.length * COL_WIDTH;
  const viewsSvgHeight =
    PADDING * 2 + rows.length * ROW_HEIGHT - GRID_VERSION_HEADER_HEIGHT;
  const modelVersionsContentWidth =
    selectedModelKey && selectedModelVersions.length > 0
      ? LABEL_WIDTH + PADDING * 2 + selectedModelVersions.length * COL_WIDTH
      : 0;

  const showSelect = modelOptions.length > TAB_THRESHOLD;
  const isLoadingModels = dataModelsStatus === "loading" || dataModelsStatus === "idle";

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <p className="text-sm text-slate-500">
          One row per view; columns are versions. Filter by data model to focus on its views.
        </p>
      </header>
      {!isLoadingModels && modelOptions.length > 0 ? (
        <div className="flex flex-col gap-2">
          <span className="text-xs font-medium text-slate-500">Data model</span>
          {showSelect ? (
            <select
              value={selectedModelKey ?? ""}
              onChange={(e) => setSelectedModelKey(e.target.value || null)}
              className="max-w-sm rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-400"
            >
              <option value="">All views</option>
              {modelOptions.map((opt) => (
                <option key={opt.key} value={opt.key}>
                  {opt.label} ({opt.viewKeys.size})
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
        </div>
      ) : null}
      {viewRows.length > 0 && !isLoadingModels ? (
        <div className="flex max-w-xl flex-col gap-1">
          <label htmlFor="view-matrix-search" className="text-xs font-medium text-slate-500">
            {t("dataCatalog.viewVersions.searchLabel")}
          </label>
          <input
            id="view-matrix-search"
            type="search"
            value={matrixSearch}
            onChange={(e) => setMatrixSearch(e.target.value)}
            placeholder={t("dataCatalog.viewVersions.searchPlaceholder")}
            autoComplete="off"
            className="rounded-md border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
          />
        </div>
      ) : null}
      <div className="flex gap-4 items-stretch">
        <div className="min-w-0 flex-1 rounded-md border border-slate-200">
        {status === "error" ? (
          <div className="flex h-64 items-center justify-center bg-red-50 text-sm text-red-700">
            {errorMessage}
          </div>
        ) : isLoading && viewRows.length === 0 ? (
          <div className="flex min-h-64 flex-col items-center justify-center gap-2 bg-sky-100 px-4 py-8 text-sm text-slate-600">
            <p className="font-medium text-slate-800">Loading views…</p>
            {loadProgress?.phase === "listing" ? (
              <p className="max-w-md text-center text-xs text-slate-500">
                Listing view definitions from CDF… {loadProgress.itemsLoaded} items fetched,{" "}
                {loadProgress.uniqueViews} unique views so far.
              </p>
            ) : null}
            {loadProgress?.phase === "details" ? (
              <p className="max-w-md text-center text-xs text-slate-500">
                Loading view details… batch {loadProgress.batchIndex} of {loadProgress.batchTotal}.
              </p>
            ) : null}
            {!loadProgress ? (
              <p className="text-xs text-slate-500">Preparing request…</p>
            ) : null}
          </div>
        ) : viewRows.length === 0 || versions.length === 0 ? (
          <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
            No views or versions found.
          </div>
        ) : modelFilteredViewRows.length === 0 ? (
          <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
            {selectedModelKey
              ? "No views in this data model."
              : "No views or versions found."}
          </div>
        ) : matrixSearch.trim() && filteredViewRows.length === 0 ? (
          <div className="flex h-64 items-center justify-center bg-sky-100 px-4 text-center text-sm text-slate-600">
            {t("dataCatalog.viewVersions.noSearchResults")}
          </div>
        ) : filteredVersions.length === 0 ? (
          <div className="flex h-64 items-center justify-center bg-sky-100 text-sm text-slate-600">
            No version data for views in this data model.
          </div>
        ) : (
          <>
            {isLoading && viewRows.length > 0 ? (
              <div className="border-b border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-950">
                {loadProgress?.phase === "listing" ? (
                  <>
                    Listing view definitions… {loadProgress.itemsLoaded} items,{" "}
                    {loadProgress.uniqueViews} unique views.
                  </>
                ) : loadProgress?.phase === "details" ? (
                  <>
                    Loading view details… batch {loadProgress.batchIndex} of {loadProgress.batchTotal}.
                  </>
                ) : (
                  <>Refreshing views…</>
                )}
              </div>
            ) : null}
            {hiddenRowCountByCap > 0 || hasMoreCatalogOnServer ? (
              <div className="flex flex-col gap-2 border-b border-slate-200 bg-white px-3 py-2 text-xs text-slate-600 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
                <div className="min-w-0 space-y-1">
                  {hiddenRowCountByCap > 0 ? (
                    <p>
                      Showing the first {INITIAL_VIEW_DISPLAY_CAP} of {legendFilteredViewRows.length}{" "}
                      views in the matrix (sorted by name).
                    </p>
                  ) : null}
                  {hasMoreCatalogOnServer ? (
                    <p>
                      Listing paused after {INITIAL_UNIQUE_VIEW_FETCH_CAP} unique views for a quick first
                      paint; more definitions exist on the server. Use{" "}
                      <span className="font-medium">Load all from server</span> to fetch the remainder.
                    </p>
                  ) : null}
                </div>
                <div className="flex shrink-0 flex-wrap gap-2">
                  {hasMoreCatalogOnServer ? (
                    <button
                      type="button"
                      className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-50 disabled:pointer-events-none disabled:opacity-50"
                      disabled={isLoading}
                      onClick={() => void loadData(true)}
                    >
                      Load all from server
                    </button>
                  ) : null}
                  {hiddenRowCountByCap > 0 ? (
                    <button
                      type="button"
                      className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-800 shadow-sm hover:bg-slate-50 disabled:pointer-events-none disabled:opacity-50"
                      disabled={isLoading}
                      onClick={() => setShowAllViewRows(true)}
                    >
                      Show all in matrix ({hiddenRowCountByCap} more)
                    </button>
                  ) : null}
                </div>
              </div>
            ) : null}
            {selectedModelKey && modelVersionRow ? (
              <div className="border-b border-slate-200">
                <div className="bg-sky-50 px-3 py-1.5 text-xs font-medium text-slate-600">
                  Data model versions
                </div>
                <div className="flex items-center gap-2 border-b border-slate-200 bg-slate-100 px-2 py-1.5">
                  <button
                    type="button"
                    className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    aria-label="Scroll left"
                    onClick={() => scrollModelVersionsBy(-Math.min(240, modelVersionsContentWidth * 0.25))}
                  >
                    ◀
                  </button>
                  <div
                    ref={modelVersionsTopRailRef}
                    className="h-4 min-h-[1rem] min-w-[120px] flex-1 cursor-grab overflow-x-auto rounded border border-slate-200 bg-white active:cursor-grabbing"
                    onScroll={() => onModelVersionsRailScroll("top")}
                  >
                    <div style={{ width: modelVersionsContentWidth, height: 1 }} aria-hidden />
                  </div>
                  <button
                    type="button"
                    className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    aria-label="Scroll right"
                    onClick={() => scrollModelVersionsBy(Math.min(240, modelVersionsContentWidth * 0.25))}
                  >
                    ▶
                  </button>
                </div>
                <div
                  ref={modelVersionsScrollRef}
                  className="overflow-x-auto"
                  onScroll={onModelVersionsMainScroll}
                  style={{
                    background: "skyblue",
                    width: `max(100%, ${modelVersionsContentWidth}px)`,
                    minWidth: "100%",
                  }}
                >
                  <svg ref={modelVersionsSvgRef} className="block" style={{ width: modelVersionsContentWidth }} />
                </div>
                <div className="flex items-center gap-2 border-t border-slate-200 bg-slate-100 px-2 py-1.5">
                  <button
                    type="button"
                    className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    aria-label="Scroll left"
                    onClick={() => scrollModelVersionsBy(-Math.min(240, modelVersionsContentWidth * 0.25))}
                  >
                    ◀
                  </button>
                  <div
                    ref={modelVersionsBottomRailRef}
                    className="h-4 min-h-[1rem] min-w-[120px] flex-1 cursor-grab overflow-x-auto rounded border border-slate-200 bg-white active:cursor-grabbing"
                    onScroll={() => onModelVersionsRailScroll("bottom")}
                  >
                    <div style={{ width: modelVersionsContentWidth, height: 1 }} aria-hidden />
                  </div>
                  <button
                    type="button"
                    className="shrink-0 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50"
                    aria-label="Scroll right"
                    onClick={() => scrollModelVersionsBy(Math.min(240, modelVersionsContentWidth * 0.25))}
                  >
                    ▶
                  </button>
                </div>
              </div>
            ) : null}
            {visibleViewLegendIds.size > 0 ? (
              <div className="bg-sky-50 px-3 py-2 text-xs text-slate-600">
                <p className="mb-2 text-[11px] text-slate-500">
                  Legend: first click shows only matching rows (include), second click hides those rows
                  (exclude), third click clears. The Implicit column counts opaque view versions not
                  shown as matrix columns.
                </p>
                <div className="flex flex-wrap items-center gap-2">
                  {VIEW_VERSION_LEGEND_ENTRIES.filter((e) => visibleViewLegendIds.has(e.id)).map(
                    ({ id, swatch, label }) => {
                      const isInclude = viewLegendFilter?.id === id && viewLegendFilter.mode === "include";
                      const isExclude = viewLegendFilter?.id === id && viewLegendFilter.mode === "exclude";
                      return (
                        <button
                          key={id}
                          type="button"
                          title={
                            isInclude
                              ? "Include: matching rows only. Click again for exclude."
                              : isExclude
                                ? "Exclude: hide matching rows. Click again to clear."
                                : "Click: include → exclude → off"
                          }
                          onClick={() => setViewLegendFilter((prev) => cycleLegendFilterState(prev, id))}
                          className={`flex max-w-[min(100%,20rem)] cursor-pointer items-center gap-2 rounded-md border-0 py-1 pl-1.5 pr-2 text-left transition ${
                            isInclude
                              ? "bg-white/95 text-slate-900 shadow-sm ring-2 ring-slate-800 ring-offset-1 ring-offset-sky-50"
                              : isExclude
                                ? "bg-rose-50/95 text-rose-950 shadow-sm ring-2 ring-rose-600 ring-offset-1 ring-offset-sky-50"
                                : "bg-transparent text-slate-600 hover:bg-white/70"
                          }`}
                        >
                          {swatch}
                          <span className="flex min-w-0 flex-col gap-0">
                            <span>{label}</span>
                            {isInclude ? (
                              <span className="text-[10px] font-medium text-slate-600">only matching</span>
                            ) : isExclude ? (
                              <span className="text-[10px] font-medium text-rose-800">hide matching</span>
                            ) : null}
                          </span>
                        </button>
                      );
                    }
                  )}
                </div>
              </div>
            ) : null}
            {legendFilteredViewRows.length === 0 && viewLegendFilter ? (
              <div className="flex min-h-48 items-center justify-center bg-sky-100 px-4 text-center text-sm text-slate-600">
                No rows match this legend setting. Click the same legend entry again to switch include
                → exclude → off.
              </div>
            ) : (
              <VersioningGridScroll
                contentWidth={viewsContentWidth}
                svgWidth={viewsContentWidth}
                bodySvgHeight={viewsSvgHeight}
                headerSvgRef={headerSvgRef}
                bodySvgRef={bodySvgRef}
              />
            )}
          </>
        )}
        </div>
        <div className="w-64 flex flex-col rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700 min-h-0 shrink-0">
          {pinnedBubble ? (
            <>
              <div className="flex items-center justify-between gap-2 shrink-0">
                <span className="font-semibold">
                  {pinnedBubble.type === "implicitVersions"
                    ? `${pinnedBubble.label} (${pinnedBubble.versions.length} implicit)`
                    : pinnedBubble.type === "view"
                      ? `${pinnedBubble.label} ${pinnedBubble.version}`
                      : `${pinnedBubble.label} ${pinnedBubble.version}`}
                </span>
                <button
                  type="button"
                  className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-slate-500 hover:bg-slate-200"
                  onClick={() => setPinnedBubble(null)}
                >
                  Unpin
                </button>
              </div>
              <p className="mt-1 text-slate-500 shrink-0">
                {pinnedBubble.type === "implicitVersions" ? "Open in CDF" : "Referrers"}
              </p>
              <ul className="mt-1 flex-1 min-h-0 space-y-1 overflow-auto pl-1">
                {referrers.length === 0 ? (
                  <li className="text-slate-500">No referrers found.</li>
                ) : (
                  referrers.map((item, i) => (
                    <li
                      key={i}
                      className={
                        item.type === "note"
                          ? "list-none pl-0 text-[11px] leading-snug text-slate-600"
                          : "flex items-center gap-1.5"
                      }
                    >
                      {item.type === "note" ? (
                        item.text
                      ) : item.type === "view" ? (
                        <span className="text-slate-500">View</span>
                      ) : item.type === "dataModel" ? (
                        <span className="text-slate-500">Data model</span>
                      ) : (
                        <span className="text-slate-500">Transformation</span>
                      )}
                      {item.type === "note" ? null : item.type === "view" ? (
                        item.url ? (
                          <a
                            href={item.url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-blue-600 hover:underline truncate"
                          >
                            {item.label}
                          </a>
                        ) : (
                          <span className="truncate">{item.label}</span>
                        )
                      ) : item.type === "transformation" ? (
                        item.url ? (
                          <a
                            href={item.url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-blue-600 hover:underline truncate"
                          >
                            {item.name}
                          </a>
                        ) : (
                          <span className="truncate">{item.name}</span>
                        )
                      ) : item.type === "dataModel" ? (
                        item.url ? (
                          <a
                            href={item.url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-blue-600 hover:underline truncate"
                          >
                            {item.label}
                          </a>
                        ) : (
                          <span className="truncate">{item.label}</span>
                        )
                      ) : null}
                    </li>
                  ))
                )}
              </ul>
            </>
          ) : (
            <div className="text-slate-500 flex-1 flex items-center">
              Click a bubble to see referrers, or a non-zero implicit count for Fusion links to those
              view versions. Pinned cells use an orange ring; indigo rings mark transformation write
              targets to the latest column.
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
