import { useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";
import { fetchContainerDetail, fetchEdgeDetail, fetchNodeDetail } from "../api";
import { PanelDragHandle } from "./PanelDragHandle";
import { PropertyViewer } from "./PropertyViewer";
import { useAppSettings } from "../context/AppSettingsContext";
import type { TreeNode } from "../types/discoveryNodes";
import type { DmInstanceKind } from "../utils/dmInstanceFromRow";
import {
  containerRefFromNodeId,
  containerRefFromNodeMeta,
  parseDmInstanceRefFromRow,
} from "../utils/dmInstanceFromRow";
import { isQueryableFileRow } from "../utils/queryableFileFromRow";
import { sqlQueryForOpenTarget } from "../utils/sqlQuerySeed";

export type PropertiesPanelLayout = "bottom" | "side" | "stacked";

type Props = {
  collapsed: boolean;
  onToggleCollapse: () => void;
  selectedNode: TreeNode | null;
  rowDetail: unknown | null;
  dmInstanceKind?: DmInstanceKind | null;
  paneSize: number;
  layout: PropertiesPanelLayout;
  isDragging?: boolean;
  onPanelDragStart: () => void;
  onPanelDragEnd: () => void;
  onQueryFile?: (row: Record<string, unknown>) => void;
};

const TREE_PREFERRED_KEYS = ["kind", "id", "label"];
const CONTAINER_PREFERRED_KEYS = [
  "kind",
  "space",
  "external_id",
  "name",
  "usedFor",
  "description",
  "properties",
  "indexes",
  "constraints",
  "queryable",
];
const INSTANCE_PREFERRED_KEYS = [
  "instance_kind",
  "space",
  "external_id",
  "type",
  "start_node",
  "end_node",
  "properties",
  "created_time",
  "last_updated_time",
];

function baseNodePayload(selectedNode: TreeNode): Record<string, unknown> {
  return {
    kind: selectedNode.kind,
    ...selectedNode.meta,
    id: selectedNode.id,
    label: selectedNode.label,
    ...(selectedNode.open_target ? { open_target: selectedNode.open_target } : {}),
  };
}

function usePropertiesPayload(
  selectedNode: TreeNode | null,
  rowDetail: unknown | null,
  dmInstanceKind: DmInstanceKind | null | undefined
): {
  payload: Record<string, unknown> | null;
  preferredKeys?: string[];
  loading: boolean;
  error: string | null;
} {
  const { t } = useAppSettings();
  const [remoteDetail, setRemoteDetail] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const rowRecord =
    rowDetail && typeof rowDetail === "object" && !Array.isArray(rowDetail)
      ? (rowDetail as Record<string, unknown>)
      : null;

  const containerRef = useMemo(() => {
    if (rowRecord || selectedNode?.kind !== "fusion_dm_container") return null;
    return (
      containerRefFromNodeMeta(selectedNode.meta) ??
      containerRefFromNodeId(selectedNode.id)
    );
  }, [rowRecord, selectedNode]);

  const instanceRef = useMemo(() => {
    if (!rowRecord || !dmInstanceKind) return null;
    return parseDmInstanceRefFromRow(rowRecord, dmInstanceKind);
  }, [rowRecord, dmInstanceKind]);

  useEffect(() => {
    if (containerRef) {
      let cancelled = false;
      setLoading(true);
      setError(null);
      setRemoteDetail(null);
      void fetchContainerDetail({ space: containerRef.space, externalId: containerRef.externalId })
        .then((detail) => {
          if (cancelled) return;
          setRemoteDetail(detail);
          setLoading(false);
        })
        .catch((e) => {
          if (cancelled) return;
          setError(String(e));
          setLoading(false);
        });
      return () => {
        cancelled = true;
      };
    }

    if (instanceRef && dmInstanceKind) {
      let cancelled = false;
      setLoading(true);
      setError(null);
      setRemoteDetail(null);
      const fetchDetail =
        dmInstanceKind === "edge"
          ? fetchEdgeDetail({ space: instanceRef.space, externalId: instanceRef.externalId })
          : fetchNodeDetail({ space: instanceRef.space, externalId: instanceRef.externalId });
      void fetchDetail
        .then((detail) => {
          if (cancelled) return;
          setRemoteDetail(detail);
          setLoading(false);
        })
        .catch((e) => {
          if (cancelled) return;
          setError(String(e));
          setLoading(false);
        });
      return () => {
        cancelled = true;
      };
    }

    setRemoteDetail(null);
    setLoading(false);
    setError(null);
    return undefined;
  }, [containerRef, instanceRef, dmInstanceKind]);

  return useMemo(() => {
    if (rowRecord) {
      const preferredKeys = instanceRef && dmInstanceKind ? INSTANCE_PREFERRED_KEYS : undefined;
      if (instanceRef && dmInstanceKind) {
        const base = remoteDetail
          ? { ...rowRecord, ...remoteDetail, instance_kind: dmInstanceKind }
          : error
            ? { ...rowRecord, instance_kind: dmInstanceKind, fetch_error: error }
            : { ...rowRecord, instance_kind: dmInstanceKind };
        return { payload: base, preferredKeys, loading, error: remoteDetail ? null : error };
      }
      return { payload: rowRecord, preferredKeys, loading: false, error: null };
    }

    if (!selectedNode) {
      return { payload: null, preferredKeys: undefined, loading: false, error: null };
    }

    if (selectedNode.kind === "fusion_dm_all") {
      const entity = selectedNode.meta?.entity;
      return {
        payload: {
          ...baseNodePayload(selectedNode),
          query_hint: selectedNode.open_target ? sqlQueryForOpenTarget(selectedNode.open_target) : null,
          hint:
            entity === "edges"
              ? t("properties.dmAllEdgesHint")
              : t("properties.dmAllNodesHint"),
        },
        preferredKeys: TREE_PREFERRED_KEYS,
        loading: false,
        error: null,
      };
    }

    if (selectedNode.kind === "fusion_dm_container") {
      const base = baseNodePayload(selectedNode);
      const payload = remoteDetail
        ? { ...base, ...remoteDetail }
        : error
          ? { ...base, fetch_error: error }
          : base;
      return {
        payload,
        preferredKeys: CONTAINER_PREFERRED_KEYS,
        loading,
        error: remoteDetail ? null : error,
      };
    }

    return {
      payload: baseNodePayload(selectedNode),
      preferredKeys: TREE_PREFERRED_KEYS,
      loading: false,
      error: null,
    };
  }, [
    rowRecord,
    selectedNode,
    instanceRef,
    dmInstanceKind,
    remoteDetail,
    loading,
    error,
    t,
  ]);
}

export function PropertiesPanel({
  collapsed,
  onToggleCollapse,
  selectedNode,
  rowDetail,
  dmInstanceKind = null,
  paneSize,
  layout,
  isDragging = false,
  onPanelDragStart,
  onPanelDragEnd,
  onQueryFile,
}: Props) {
  const { t } = useAppSettings();
  const { payload, preferredKeys, loading, error } = usePropertiesPayload(
    selectedNode,
    rowDetail,
    dmInstanceKind
  );

  const layoutClass =
    layout === "side"
      ? "disc-properties-pane--side"
      : layout === "stacked"
        ? "disc-properties-pane--stacked"
        : "disc-properties-pane--bottom";

  const sizeStyle: CSSProperties =
    layout === "side"
      ? { width: paneSize, minWidth: paneSize, maxWidth: paneSize, height: "100%" }
      : collapsed
        ? { height: "auto" }
        : { height: paneSize };

  const queryableRow =
    rowDetail && typeof rowDetail === "object" && isQueryableFileRow(rowDetail as Record<string, unknown>)
      ? (rowDetail as Record<string, unknown>)
      : null;

  return (
    <div
      className={`disc-properties-pane ${layoutClass}${collapsed ? " disc-properties-pane--collapsed" : ""}${isDragging ? " disc-panel--dragging" : ""}`}
      style={sizeStyle}
    >
      <div className="disc-properties-header">
        <PanelDragHandle
          panel="properties"
          labelKey="layout.dragHandle.properties"
          onDragStart={() => onPanelDragStart()}
          onDragEnd={onPanelDragEnd}
        />
        <span className="disc-properties-header__title">
          {t("properties.title")}
          {!collapsed ? ` — ${rowDetail ? t("properties.row") : t("properties.node")}` : ""}
        </span>
        <div className="disc-properties-header__actions">
          {!collapsed && queryableRow && onQueryFile && (
            <button type="button" className="disc-btn" onClick={() => onQueryFile(queryableRow)}>
              {t("sql.queryFile")}
            </button>
          )}
          <button type="button" className="disc-btn" onClick={onToggleCollapse}>
            {collapsed ? t("properties.show") : t("properties.collapse")}
          </button>
        </div>
      </div>
      {!collapsed && (
        <div className="disc-properties-body">
          {loading && <p className="disc-empty-hint">{t("properties.loading")}</p>}
          {!loading && error && !payload && (
            <p className="disc-banner--error">{error}</p>
          )}
          {payload ? (
            <PropertyViewer value={payload} preferredKeys={preferredKeys} />
          ) : !loading ? (
            <p className="disc-empty-hint">{t("properties.emptyHint")}</p>
          ) : null}
        </div>
      )}
    </div>
  );
}
