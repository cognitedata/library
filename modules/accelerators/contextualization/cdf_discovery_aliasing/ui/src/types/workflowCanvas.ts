/**
 * Serialized flow graph under `canvas` in a v1 scope YAML (or root `nodes`/`edges` when parsing a canvas-only blob).
 * paired with the scope document (canonical config remains in workflow *.config.yaml).
 */

export const WORKFLOW_CANVAS_SCHEMA_VERSION = 1;

/** Edge handle placement and default auto-layout primary axis. */
export type WorkflowCanvasHandleOrientation = "lr" | "tb";

export function normalizeWorkflowCanvasHandleOrientation(raw: unknown): WorkflowCanvasHandleOrientation {
  if (raw === "tb") return "tb";
  return "lr";
}

export type CanvasNodeRfType =
  | "keaStart"
  | "keaEnd"
  | "keaSourceView"
  | "keaDiscoveryValidate"
  | "keaDiscoveryInstanceFilter"
  | "keaDiscoveryConfidenceFilter"
  | "keaViewQuery"
  | "keaRawQuery"
  | "keaClassicQuery"
  | "keaTransform"
  | "keaMerge"
  | "keaJoin"
  | "keaViewSave"
  | "keaRawSave"
  | "keaClassicSave"
  | "keaAliasPersistence"
  | "keaInvertedIndex"
  | "keaMatchValidationRuleSourceView"
  | "keaMatchValidationRuleExtraction"
  | "keaMatchValidationRuleAliasing"
  | "keaSubflowGraphIn"
  | "keaSubflowGraphOut"
  | "keaSubgraph";

/** Prefix for subflow boundary / hub handles: external ``→ subflow`` uses ``in__{portId}`` targets. */
export const SUBFLOW_PORT_HANDLE_IN_PREFIX = "in__";
/** Prefix: internal sources from input hub / subflow boundary outputs use ``out__{portId}``. */
export const SUBFLOW_PORT_HANDLE_OUT_PREFIX = "out__";

export function subflowTargetHandleForPort(portId: string): string {
  return `${SUBFLOW_PORT_HANDLE_IN_PREFIX}${portId}`;
}

export function subflowSourceHandleForPort(portId: string): string {
  return `${SUBFLOW_PORT_HANDLE_OUT_PREFIX}${portId}`;
}

export function parsePortIdFromSubflowTargetHandle(h: string | null | undefined): string | null {
  if (h == null) return null;
  const s = String(h);
  return s.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX) ? s.slice(SUBFLOW_PORT_HANDLE_IN_PREFIX.length) : null;
}

export function parsePortIdFromSubflowSourceHandle(h: string | null | undefined): string | null {
  if (h == null) return null;
  const s = String(h);
  return s.startsWith(SUBFLOW_PORT_HANDLE_OUT_PREFIX) ? s.slice(SUBFLOW_PORT_HANDLE_OUT_PREFIX.length) : null;
}

export function isSubflowGraphHubRfType(t: string | undefined): boolean {
  return t === "keaSubflowGraphIn" || t === "keaSubflowGraphOut";
}

export function isSubflowGraphHubKind(k: CanvasNodeKind): boolean {
  return k === "subflow_graph_in" || k === "subflow_graph_out";
}

export type SubflowPortEntry = {
  id: string;
  label?: string;
  /**
   * React Flow ``type`` of the inner node this frame input feeds (set when ports are inferred from
   * crossings). Parent→subgraph wiring is validated as if connecting to that node’s ``in``.
   */
  inner_target_rf_type?: string;
  /**
   * React Flow ``type`` of the inner node this frame output originates from. Subgraph→parent
   * wiring is validated as if connecting from that node’s main data ``out``.
   */
  inner_source_rf_type?: string;
};

export type SubflowPortsConfig = { inputs: SubflowPortEntry[]; outputs: SubflowPortEntry[] };

/** Optional named profile resolved from scope ``persistence_profiles[]`` at compile time. */
export type PersistenceProfileRef = {
  profile?: string;
};

/** Executable persistence settings for ``kind: alias_persistence`` nodes (merged into IR ``persistence``). */
export type AliasPersistenceConfig = PersistenceProfileRef & {
  kind: "alias_persistence";
  raw_db?: string;
  raw_table_aliases?: string;
  raw_read_limit?: number;
  source_raw_db?: string;
  source_raw_table_key?: string;
  source_raw_read_limit?: number;
  incremental_auto_run_id?: boolean;
  incremental_transition?: boolean;
  source_view_space?: string;
  source_view_external_id?: string;
  source_view_version?: string;
  write_foreign_key_references?: boolean;
  foreign_key_writeback_property?: string;
};

/** Inverted-index RAW sink for ``kind: inverted_index`` nodes. */
export type InvertedIndexPersistenceConfig = PersistenceProfileRef & {
  kind: "inverted_index";
  source_raw_db?: string;
  source_raw_table_key?: string;
  source_raw_read_limit?: number;
  inverted_index_raw_db?: string;
  inverted_index_raw_table?: string;
  inverted_index_fk_entity_type?: string;
  inverted_index_document_entity_type?: string;
  source_view_space?: string;
  source_view_external_id?: string;
  source_view_version?: string;
};

export type PersistenceConfig = AliasPersistenceConfig | InvertedIndexPersistenceConfig;

export type CanvasEdgeKind = "data" | "sequence" | "parallel_group";

/** Logical kind stored in canvas file (maps to React Flow custom type). */
export type CanvasNodeKind =
  | "start"
  | "end"
  | "source_view"
  | "validation"
  | "instance_filter"
  | "confidence_filter"
  | "query_view"
  | "query_raw"
  | "query_classic"
  | "transform"
  | "merge"
  | "join"
  | "save_view"
  | "save_raw"
  | "save_classic"
  | "alias_persistence"
  | "inverted_index"
  | "match_validation_source_view"
  | "match_validation_extraction"
  | "match_validation_aliasing"
  | "subflow_graph_in"
  | "subflow_graph_out"
  | "subgraph";

export interface CanvasNodeRef {
  /** Index into source_views[] */
  source_view_index?: number;
  /** Match key for source view */
  view_space?: string;
  view_external_id?: string;
  view_version?: string;
  extraction_global_validation?: boolean;
  aliasing_global_validation?: boolean;
  shared_source_view_validation_chain?: boolean;
  source_view_indices?: number[];
  validation_list_key?: string;
}

export interface WorkflowCanvasNodeData {
  label?: string;
  /** extraction | aliasing | annotation | persistence | incremental — which handler family */
  handler_family?: "annotation" | "persistence" | "incremental" | "discovery";
  /** View save vs inverted index (layout / compile hints) */
  persistence_step?: "alias_writeback" | "inverted_index";
  /** View query — cohort rows and RUN_ID before transform when incremental is on */
  incremental_step?: "state_update";
  /** e.g. regex_handler, heuristic_sampler, character_substitution */
  handler_id?: string;
  /** True when dropped from a preset palette item */
  preset_from_palette?: boolean;
  ref?: CanvasNodeRef;
  notes?: string;
  /**
   * Optional CSS color for the node card’s left accent (hex/rgb/hsl). Layout-only; not used by the workflow engine.
   */
  node_color?: string;
  /**
   * Optional CSS color for the node card background (hex/rgb/hsl). Layout-only; not used by the workflow engine.
   */
  node_bg_color?: string;
  /** validation / annotation sub-kind (layout overlays — not validation_rules) */
  annotation_kind?: "global_validation" | "edge_validation";
  /**
   * Confidence-match validation rule (`validation_rules[]`) — where it is evaluated in scope YAML.
   */
  validation_rule_context?: "source_view" | "extraction" | "aliasing";
  /** Name of the `validation_rules` entry (paired with ref.* parent). */
  validation_rule_name?: string;
  /**
   * Optional index for auto-layout: within one graph layer, transform nodes sort by this
   * (ascending) instead of id-only.
   */
  pipeline_rank?: number;
  /** Named subgraph ports; drives frame handles + internal hub handles. */
  subflow_ports?: SubflowPortsConfig;
  /** Child node id — input hub inside this subgraph (sources per input port). */
  subflow_hub_input_id?: string;
  /** Child node id — output hub inside this subgraph (targets per output port). */
  subflow_hub_output_id?: string;
  /**
   * Discovery pipeline stage settings (``query_view``, ``transform``, ``save_view``, …).
   * Shape depends on ``kind``; compiled into task ``payload.config``.
   */
  config?: Record<string, unknown>;
  /**
   * Nested canvas for ``kind: subgraph`` — edited in a drill-in view; not rendered as
   * ``parentId`` children on the outer graph.
   */
  inner_canvas?: WorkflowCanvasDocument;
  /**
   * Per-node persistence settings for ``alias_persistence`` / ``inverted_index``.
   * Compiler merges with ``persistence_profiles`` and scope defaults into IR ``persistence``.
   */
  persistence_config?: PersistenceConfig;
  /** React Flow mirror of node-level ``enabled`` (not persisted in scope YAML). */
  canvas_node_enabled?: boolean;
  /** React Flow mirror of ``cascade_disabled`` (auto-disabled downstream; not in ``data.config``). */
  canvas_node_cascade_disabled?: boolean;
}

export interface WorkflowCanvasNode {
  id: string;
  kind: CanvasNodeKind;
  position: { x: number; y: number };
  data: WorkflowCanvasNodeData;
  /**
   * When false, the node stays on the canvas but is omitted from compiled workflow IR.
   * Distinct from ``data.config.enabled`` on transform/validation handlers.
   */
  enabled?: boolean;
  /**
   * When true with ``enabled: false``, the node was disabled automatically because upstream
   * executables were turned off (re-enabled when upstream is restored).
   */
  cascade_disabled?: boolean;
  /** Optional React Flow parent id when serializing nested groups (subgraph inner graph uses inner_canvas, not parent_id). */
  parent_id?: string | null;
  /** Optional persisted frame size (e.g. for future layout hints). */
  size?: { width: number; height: number };
}

/** True when the node participates in workflow compile (default true). */
export function isWorkflowCanvasNodeEnabled(
  node: Pick<WorkflowCanvasNode, "enabled"> | { enabled?: boolean }
): boolean {
  return node.enabled !== false;
}

export function isWorkflowCanvasNodeCascadeDisabled(
  node: Pick<WorkflowCanvasNode, "cascade_disabled"> | { cascade_disabled?: boolean }
): boolean {
  return node.cascade_disabled === true;
}

export interface WorkflowCanvasEdge {
  id: string;
  source: string;
  target: string;
  source_handle?: string | null;
  target_handle?: string | null;
  kind?: CanvasEdgeKind;
}

export interface WorkflowCanvasDocument {
  schemaVersion: number;
  nodes: WorkflowCanvasNode[];
  edges: WorkflowCanvasEdge[];
  /** When set, node handles and auto-layout follow this axis (default ``lr``). */
  handle_orientation?: WorkflowCanvasHandleOrientation;
}

export function emptyWorkflowCanvasDocument(): WorkflowCanvasDocument {
  return {
    schemaVersion: WORKFLOW_CANVAS_SCHEMA_VERSION,
    nodes: [],
    edges: [],
  };
}

function isMatchValidationCanvasKind(k: CanvasNodeKind | undefined): boolean {
  return (
    k === "match_validation_source_view" ||
    k === "match_validation_extraction" ||
    k === "match_validation_aliasing"
  );
}

/**
 * Ensures persisted canvas edges use React Flow handle ids that match node components
 * (``out`` / ``in`` / ``validation``, and ``in__`` / ``out__`` for subgraph ports).
 * Safe to call after parse or when hydrating from scope seed.
 */
export function normalizeWorkflowCanvasEdgeHandles(
  nodes: WorkflowCanvasNode[],
  edges: WorkflowCanvasEdge[]
): WorkflowCanvasEdge[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  return edges.map((e) => normalizeOneWorkflowCanvasEdge(e, byId));
}

function normalizeOneWorkflowCanvasEdge(
  e: WorkflowCanvasEdge,
  byId: Map<string, WorkflowCanvasNode>
): WorkflowCanvasEdge {
  const src = byId.get(e.source);
  const tgt = byId.get(e.target);
  if (!src || !tgt) return e;

  const sh0 = e.source_handle != null ? String(e.source_handle) : "";
  const th0 = e.target_handle != null ? String(e.target_handle) : "";
  const srcSubOut = sh0.startsWith(SUBFLOW_PORT_HANDLE_OUT_PREFIX);
  const tgtSubIn = th0.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX);

  const k = e.kind ?? "data";

  if (k === "sequence") {
    if (isMatchValidationCanvasKind(src.kind) && isMatchValidationCanvasKind(tgt.kind)) {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    if (src.kind === "transform" && tgt.kind === "transform") {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    if (src.kind === "join" && tgt.kind === "join") {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    if (src.kind === "validation" && tgt.kind === "validation") {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    if (src.kind === "instance_filter" && tgt.kind === "instance_filter") {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    return e;
  }

  if (k !== "data") return e;

  if (src.kind === "source_view" && tgt.kind === "match_validation_source_view") {
    if (!sh0 || sh0 === "validation") {
      return { ...e, source_handle: "out", target_handle: th0 || "in" };
    }
    if (!th0) {
      return { ...e, target_handle: "in" };
    }
  }

  const stagesToMatchExValidation: CanvasNodeKind[] = [
    "save_view",
    "save_raw",
    "save_classic",
    "query_view",
    "query_raw",
    "query_classic",
    "transform",
    "merge",
    "join",
    "validation",
    "instance_filter",
    "confidence_filter",
    "inverted_index",
    "alias_persistence",
  ];
  for (const sk of stagesToMatchExValidation) {
    if (src.kind === sk && tgt.kind === "match_validation_extraction" && !srcSubOut) {
      if (sk === "validation") {
        if (!sh0 || sh0 === "out" || sh0 === "validation") {
          return { ...e, source_handle: "out", target_handle: th0 || "in" };
        }
        if (!th0) {
          return { ...e, target_handle: "in" };
        }
      } else if (!sh0 || sh0 === "out") {
        return { ...e, source_handle: "validation", target_handle: th0 || "in" };
      } else if (!th0) {
        return { ...e, target_handle: "in" };
      }
    }
  }

  if (!srcSubOut && !tgtSubIn && defaultCanvasDataEdgeUsesOutIn(src.kind, tgt.kind)) {
    if (!sh0 && !th0) {
      return { ...e, source_handle: "out", target_handle: "in" };
    }
    if (sh0 && !th0 && canvasTargetWantsDefaultIn(tgt.kind)) {
      return { ...e, target_handle: "in" };
    }
    if (!sh0 && th0 === "in" && canvasSourceWantsDefaultOut(src.kind)) {
      return { ...e, source_handle: "out" };
    }
  }

  return e;
}

function isDataPipelineStageKind(k: CanvasNodeKind): boolean {
  return (
    k === "validation" ||
    k === "instance_filter" ||
    k === "confidence_filter" ||
    k === "save_view" ||
    k === "save_raw" ||
    k === "save_classic" ||
    k === "query_view" ||
    k === "query_raw" ||
    k === "query_classic" ||
    k === "transform" ||
    k === "merge" ||
    k === "join" ||
    k === "alias_persistence" ||
    k === "inverted_index"
  );
}

function canvasSourceWantsDefaultOut(sk: CanvasNodeKind): boolean {
  if (sk === "start") return true;
  if (sk === "source_view") return true;
  if (isDataPipelineStageKind(sk)) return true;
  if (isMatchValidationCanvasKind(sk)) return true;
  return false;
}

function canvasTargetWantsDefaultIn(tk: CanvasNodeKind): boolean {
  if (tk === "end") return true;
  if (tk === "source_view") return true;
  if (isDataPipelineStageKind(tk)) return true;
  if (isMatchValidationCanvasKind(tk)) return true;
  return false;
}

function defaultCanvasDataEdgeUsesOutIn(sk: CanvasNodeKind, tk: CanvasNodeKind): boolean {
  if (tk === "join") return false;
  if (sk === "source_view" && tk === "match_validation_source_view") return false;

  if (sk === "start" && tk === "source_view") return true;
  if (sk === "start" && (tk === "query_view" || tk === "query_raw" || tk === "query_classic")) return true;
  if (sk === "source_view" && tk === "query_view") return true;
  if (isDataPipelineStageKind(sk) && isDataPipelineStageKind(tk)) return true;
  if (isDataPipelineStageKind(sk) && tk === "end") return true;
  if (isMatchValidationCanvasKind(sk) && tk === "end") return true;
  return false;
}

/** Recursively normalizes ``edges`` on ``doc`` and every ``subgraph`` ``inner_canvas``. */
export function normalizeWorkflowCanvasDocumentEdgeHandles(doc: WorkflowCanvasDocument): void {
  doc.edges = normalizeWorkflowCanvasEdgeHandles(doc.nodes, doc.edges);
  for (const n of doc.nodes) {
    if (n.kind === "subgraph" && n.data.inner_canvas?.nodes?.length) {
      normalizeWorkflowCanvasDocumentEdgeHandles(n.data.inner_canvas);
    }
  }
}

export function parseWorkflowCanvasDocument(raw: unknown): WorkflowCanvasDocument {
  const empty = emptyWorkflowCanvasDocument();
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return empty;
  const o = raw as Record<string, unknown>;
  const sv = o.schemaVersion;
  const schemaVersion =
    typeof sv === "number" && Number.isFinite(sv) ? Math.floor(sv) : WORKFLOW_CANVAS_SCHEMA_VERSION;
  const nodesRaw = o.nodes;
  const edgesRaw = o.edges;
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];

  if (Array.isArray(nodesRaw)) {
    for (const n of nodesRaw) {
      if (!n || typeof n !== "object" || Array.isArray(n)) continue;
      const node = n as Record<string, unknown>;
      const id = String(node.id ?? "").trim();
      if (!id) continue;
      const kindRaw = node.kind as CanvasNodeKind | undefined;
      const kind = kindRaw;
      if (kindRaw === "extraction" || kindRaw === "aliasing") {
        continue;
      }
      if (
        kind !== "start" &&
        kind !== "end" &&
        kind !== "source_view" &&
        kind !== "validation" &&
        kind !== "instance_filter" &&
        kind !== "confidence_filter" &&
        kind !== "query_view" &&
        kind !== "query_raw" &&
        kind !== "query_classic" &&
        kind !== "transform" &&
        kind !== "merge" &&
        kind !== "join" &&
        kind !== "save_view" &&
        kind !== "save_raw" &&
        kind !== "save_classic" &&
        kind !== "alias_persistence" &&
        kind !== "inverted_index" &&
        kind !== "match_validation_source_view" &&
        kind !== "match_validation_extraction" &&
        kind !== "match_validation_aliasing" &&
        kind !== "subflow_graph_in" &&
        kind !== "subflow_graph_out" &&
        kind !== "subgraph"
      ) {
        continue;
      }
      const pos = node.position;
      let position = { x: 0, y: 0 };
      if (pos && typeof pos === "object" && !Array.isArray(pos)) {
        const px = (pos as Record<string, unknown>).x;
        const py = (pos as Record<string, unknown>).y;
        position = {
          x: typeof px === "number" ? px : 0,
          y: typeof py === "number" ? py : 0,
        };
      }
      const dataRaw = node.data;
      const data: WorkflowCanvasNodeData =
        dataRaw !== null && typeof dataRaw === "object" && !Array.isArray(dataRaw)
          ? (dataRaw as WorkflowCanvasNodeData)
          : {};
      const parentRaw = node.parent_id;
      const parent_id =
        parentRaw != null && String(parentRaw).trim() ? String(parentRaw).trim() : undefined;
      const sizeRaw = node.size;
      let size: { width: number; height: number } | undefined;
      if (sizeRaw && typeof sizeRaw === "object" && !Array.isArray(sizeRaw)) {
        const sw = (sizeRaw as Record<string, unknown>).width;
        const sh = (sizeRaw as Record<string, unknown>).height;
        const w = typeof sw === "number" && Number.isFinite(sw) ? Math.max(1, Math.floor(sw)) : 0;
        const h = typeof sh === "number" && Number.isFinite(sh) ? Math.max(1, Math.floor(sh)) : 0;
        if (w > 0 && h > 0) size = { width: w, height: h };
      }
      let entryData: WorkflowCanvasNodeData = data;
      if (kind === "subgraph" && dataRaw !== null && typeof dataRaw === "object" && !Array.isArray(dataRaw)) {
        const icRaw = (dataRaw as Record<string, unknown>).inner_canvas;
        if (icRaw !== null && typeof icRaw === "object" && !Array.isArray(icRaw)) {
          entryData = { ...data, inner_canvas: parseWorkflowCanvasDocument(icRaw) };
        } else if (!data.inner_canvas) {
          entryData = { ...data, inner_canvas: emptyWorkflowCanvasDocument() };
        }
      }

      const enabledRaw = node.enabled;
      const enabled = enabledRaw === false ? false : undefined;
      const cascadeRaw = node.cascade_disabled;
      const cascade_disabled = cascadeRaw === true ? true : undefined;
      const entry: WorkflowCanvasNode = { id, kind, position, data: entryData };
      if (enabled === false) entry.enabled = false;
      if (cascade_disabled === true) entry.cascade_disabled = true;
      if (parent_id) entry.parent_id = parent_id;
      if (size) entry.size = size;
      nodes.push(entry);
    }
  }

  if (Array.isArray(edgesRaw)) {
    let syntheticEdgeSeq = 0;
    const edgeIdsSeen = new Set<string>();
    for (const e of edgesRaw) {
      if (!e || typeof e !== "object" || Array.isArray(e)) continue;
      const edge = e as Record<string, unknown>;
      let id = String(edge.id ?? "").trim();
      const source = String(edge.source ?? "").trim();
      const target = String(edge.target ?? "").trim();
      if (!source || !target) continue;
      if (!id) {
        id = `e_${source}_to_${target}`;
        while (edgeIdsSeen.has(id)) {
          syntheticEdgeSeq += 1;
          id = `e_${source}_to_${target}_${syntheticEdgeSeq}`;
        }
      }
      edgeIdsSeen.add(id);
      const kind = edge.kind as CanvasEdgeKind | undefined;
      const ek =
        kind === "sequence" || kind === "parallel_group" || kind === "data" ? kind : "data";
      edges.push({
        id,
        source,
        target,
        source_handle: edge.source_handle != null ? String(edge.source_handle) : undefined,
        target_handle: edge.target_handle != null ? String(edge.target_handle) : undefined,
        kind: ek,
      });
    }
  }

  const nodeIds = new Set(nodes.map((n) => n.id));
  const filteredEdges = edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const edgesNormalized = filteredEdges.map((e) => {
    const src = byId.get(e.source);
    const sh = e.source_handle != null ? String(e.source_handle) : "";
    return e;
  });
  const doc: WorkflowCanvasDocument = {
    schemaVersion,
    nodes,
    edges: edgesNormalized,
    handle_orientation: normalizeWorkflowCanvasHandleOrientation(o.handle_orientation),
  };
  normalizeWorkflowCanvasDocumentEdgeHandles(doc);
  return doc;
}

export function kindToRfType(kind: CanvasNodeKind): CanvasNodeRfType {
  switch (kind) {
    case "start":
      return "keaStart";
    case "end":
      return "keaEnd";
    case "source_view":
      return "keaSourceView";
    case "validation":
      return "keaDiscoveryValidate";
    case "instance_filter":
      return "keaDiscoveryInstanceFilter";
    case "confidence_filter":
      return "keaDiscoveryConfidenceFilter";
    case "query_view":
      return "keaViewQuery";
    case "query_raw":
      return "keaRawQuery";
    case "query_classic":
      return "keaClassicQuery";
    case "transform":
      return "keaTransform";
    case "merge":
      return "keaMerge";
    case "join":
      return "keaJoin";
    case "save_view":
      return "keaViewSave";
    case "save_raw":
      return "keaRawSave";
    case "save_classic":
      return "keaClassicSave";
    case "alias_persistence":
      return "keaAliasPersistence";
    case "inverted_index":
      return "keaInvertedIndex";
    case "match_validation_source_view":
      return "keaMatchValidationRuleSourceView";
    case "match_validation_extraction":
      return "keaMatchValidationRuleExtraction";
    case "match_validation_aliasing":
      return "keaMatchValidationRuleAliasing";
    case "subflow_graph_in":
      return "keaSubflowGraphIn";
    case "subflow_graph_out":
      return "keaSubflowGraphOut";
    case "subgraph":
      return "keaSubgraph";
    default:
      return "keaTransform";
  }
}

export function rfTypeToKind(t: string | undefined): CanvasNodeKind {
  switch (t) {
    case "keaStart":
      return "start";
    case "keaEnd":
      return "end";
    case "keaSourceView":
      return "source_view";
    case "keaDiscoveryValidate":
      return "validation";
    case "keaDiscoveryInstanceFilter":
      return "instance_filter";
    case "keaDiscoveryConfidenceFilter":
      return "confidence_filter";
    case "keaViewQuery":
      return "query_view";
    case "keaRawQuery":
      return "query_raw";
    case "keaClassicQuery":
      return "query_classic";
    case "keaTransform":
      return "transform";
    case "keaMerge":
      return "merge";
    case "keaJoin":
      return "join";
    case "keaViewSave":
      return "save_view";
    case "keaRawSave":
      return "save_raw";
    case "keaClassicSave":
      return "save_classic";
    case "keaAliasPersistence":
      return "alias_persistence";
    case "keaInvertedIndex":
      return "inverted_index";
    case "keaMatchValidationRuleSourceView":
      return "match_validation_source_view";
    case "keaMatchValidationRuleExtraction":
      return "match_validation_extraction";
    case "keaMatchValidationRuleAliasing":
      return "match_validation_aliasing";
    case "keaSubflowGraphIn":
      return "subflow_graph_in";
    case "keaSubflowGraphOut":
      return "subflow_graph_out";
    case "keaSubgraph":
      return "subgraph";
    default:
      return "transform";
  }
}
