/**
 * workflow.local.canvas.yaml / workflow.template.canvas.yaml — layout-only document
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
  | "keaExtraction"
  | "keaAliasing"
  | "keaValidation"
  | "keaAliasPersistence"
  | "keaWritebackRaw"
  | "keaWritebackDataModeling"
  | "keaReferenceIndex"
  | "keaMatchValidationRuleSourceView"
  | "keaMatchValidationRuleExtraction"
  | "keaMatchValidationRuleAliasing"
  | "keaSubflow"
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

export type CanvasEdgeKind = "data" | "sequence" | "parallel_group";

/** Logical kind stored in canvas file (maps to React Flow custom type). */
export type CanvasNodeKind =
  | "start"
  | "end"
  | "source_view"
  | "extraction"
  | "aliasing"
  | "validation"
  | "alias_persistence"
  | "writeback_raw"
  | "writeback_data_modeling"
  | "reference_index"
  | "match_validation_source_view"
  | "match_validation_extraction"
  | "match_validation_aliasing"
  | "subflow"
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
  extraction_rule_name?: string;
  extraction_rule_id?: string;
  aliasing_rule_name?: string;
  /** Match-validation node for `key_extraction.config.data.validation` (not a single extraction rule). */
  extraction_global_validation?: boolean;
  /** Match-validation node for `aliasing.config.data.validation`. */
  aliasing_global_validation?: boolean;
  /**
   * Match-rule node is shared by multiple extraction rules with identical validation lists (canvas seed).
   */
  shared_extraction_validation_chain?: boolean;
  /** Extraction rule names participating in a shared chain (sorted). */
  extraction_rule_names?: string[];
  /**
   * Match-rule node is shared by multiple aliasing rules with identical validation lists (canvas seed).
   */
  shared_aliasing_validation_chain?: boolean;
  /** Aliasing rule names participating in a shared chain (sorted). */
  aliasing_rule_names?: string[];
  /**
   * Match-rule node is shared by multiple source views with identical validation lists (canvas seed).
   */
  shared_source_view_validation_chain?: boolean;
  /** Source view indices participating in a shared chain (sorted). */
  source_view_indices?: number[];
  /** Stable id segment tying a shared chain to its ordered rule-name list (seed / layout). */
  validation_list_key?: string;
}

export interface WorkflowCanvasNodeData {
  label?: string;
  /** extraction | aliasing | annotation | persistence | incremental — which handler family */
  handler_family?: "extraction" | "aliasing" | "annotation" | "persistence" | "incremental";
  /** fn_dm_alias_persistence vs fn_dm_reference_index (layout / future compile) */
  persistence_step?: "alias_writeback" | "reference_index";
  /**
   * Palette writeback cards: where results are intended to land (RAW vs Data Modeling).
   * Layout-only; actual sinks are configured in scope or task data.
   */
  writeback_sink?: "raw" | "data_modeling";
  /** fn_dm_incremental_state_update — cohort rows before key extraction when incremental is on */
  incremental_step?: "state_update";
  /** e.g. regex_handler, heuristic, character_substitution */
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
  /** Named subgraph ports (``kind: subflow``); drives frame handles + internal hub handles. */
  subflow_ports?: SubflowPortsConfig;
  /** Child node id — input hub inside this subflow (sources per input port). */
  subflow_hub_input_id?: string;
  /** Child node id — output hub inside this subflow (targets per output port). */
  subflow_hub_output_id?: string;
  /**
   * Nested canvas for ``kind: subgraph`` — edited in a drill-in view; not rendered as
   * ``parentId`` children on the outer graph.
   */
  inner_canvas?: WorkflowCanvasDocument;
}

export interface WorkflowCanvasNode {
  id: string;
  kind: CanvasNodeKind;
  position: { x: number; y: number };
  data: WorkflowCanvasNodeData;
  /** When set, this node is drawn inside the subflow parent (coordinates are relative to the parent). */
  parent_id?: string | null;
  /** Bounding size for ``kind: subflow`` (persisted; drives React Flow group dimensions). */
  size?: { width: number; height: number };
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
    if (src.kind === "aliasing" && tgt.kind === "aliasing") {
      return {
        ...e,
        source_handle: srcSubOut ? e.source_handle : "out",
        target_handle: tgtSubIn ? e.target_handle : "in",
      };
    }
    return e;
  }

  if (k !== "data") return e;

  if (src.kind === "extraction" && tgt.kind === "match_validation_extraction" && !srcSubOut) {
    if (!sh0 || sh0 === "out") {
      return { ...e, source_handle: "validation", target_handle: th0 || "in" };
    }
    if (!th0) {
      return { ...e, target_handle: "in" };
    }
  }
  if (src.kind === "aliasing" && tgt.kind === "match_validation_aliasing" && !srcSubOut) {
    if (!sh0 || sh0 === "out") {
      return { ...e, source_handle: "validation", target_handle: th0 || "in" };
    }
    if (!th0) {
      return { ...e, target_handle: "in" };
    }
  }
  if (src.kind === "source_view" && tgt.kind === "match_validation_source_view") {
    if (!sh0 || sh0 === "validation") {
      return { ...e, source_handle: "out", target_handle: th0 || "in" };
    }
    if (!th0) {
      return { ...e, target_handle: "in" };
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

function canvasSourceWantsDefaultOut(sk: CanvasNodeKind): boolean {
  if (sk === "start") return true;
  if (sk === "source_view") return true;
  if (sk === "extraction") return true;
  if (sk === "aliasing") return true;
  if (sk === "validation") return true;
  if (sk === "writeback_raw" || sk === "writeback_data_modeling") return true;
  if (isMatchValidationCanvasKind(sk)) return true;
  return false;
}

function canvasTargetWantsDefaultIn(tk: CanvasNodeKind): boolean {
  if (tk === "end") return true;
  if (tk === "source_view") return true;
  if (tk === "extraction") return true;
  if (tk === "aliasing") return true;
  if (tk === "validation") return true;
  if (tk === "writeback_raw" || tk === "writeback_data_modeling") return true;
  if (isMatchValidationCanvasKind(tk)) return true;
  return false;
}

function defaultCanvasDataEdgeUsesOutIn(sk: CanvasNodeKind, tk: CanvasNodeKind): boolean {
  if (sk === "extraction" && tk === "match_validation_extraction") return false;
  if (sk === "aliasing" && tk === "match_validation_aliasing") return false;
  if (sk === "source_view" && tk === "match_validation_source_view") return false;

  if (sk === "start" && (tk === "source_view" || tk === "extraction")) return true;
  if (sk === "source_view" && tk === "extraction") return true;
  if (sk === "extraction" && tk === "aliasing") return true;
  if (sk === "extraction" && (tk === "writeback_raw" || tk === "writeback_data_modeling")) return true;
  if (sk === "aliasing" && (tk === "writeback_raw" || tk === "writeback_data_modeling")) return true;
  if (sk === "validation" && (tk === "writeback_raw" || tk === "writeback_data_modeling")) return true;
  if (sk === "extraction" && tk === "end") return true;
  if (sk === "aliasing" && tk === "end") return true;
  if (
    (sk === "writeback_raw" || sk === "writeback_data_modeling") &&
    tk === "end"
  )
    return true;
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
      const kind = node.kind as CanvasNodeKind | undefined;
      if (
        kind !== "start" &&
        kind !== "end" &&
        kind !== "source_view" &&
        kind !== "extraction" &&
        kind !== "aliasing" &&
        kind !== "validation" &&
        kind !== "alias_persistence" &&
        kind !== "writeback_raw" &&
        kind !== "writeback_data_modeling" &&
        kind !== "reference_index" &&
        kind !== "match_validation_source_view" &&
        kind !== "match_validation_extraction" &&
        kind !== "match_validation_aliasing" &&
        kind !== "subflow" &&
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

      const entry: WorkflowCanvasNode = { id, kind, position, data: entryData };
      if (parent_id) entry.parent_id = parent_id;
      if (size) entry.size = size;
      nodes.push(entry);
    }
  }

  if (Array.isArray(edgesRaw)) {
    for (const e of edgesRaw) {
      if (!e || typeof e !== "object" || Array.isArray(e)) continue;
      const edge = e as Record<string, unknown>;
      const id = String(edge.id ?? "").trim();
      const source = String(edge.source ?? "").trim();
      const target = String(edge.target ?? "").trim();
      if (!id || !source || !target) continue;
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
    /** Strip legacy mistaken ``out_*`` ids; keep ``out__`` subgraph port sources. */
    const legacyMalformedExtractionOut =
      src?.kind === "extraction" &&
      sh.startsWith("out_") &&
      !sh.startsWith(SUBFLOW_PORT_HANDLE_OUT_PREFIX);
    if (legacyMalformedExtractionOut) {
      return { ...e, source_handle: undefined };
    }
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
    case "extraction":
      return "keaExtraction";
    case "aliasing":
      return "keaAliasing";
    case "validation":
      return "keaValidation";
    case "alias_persistence":
      return "keaAliasPersistence";
    case "writeback_raw":
      return "keaWritebackRaw";
    case "writeback_data_modeling":
      return "keaWritebackDataModeling";
    case "reference_index":
      return "keaReferenceIndex";
    case "match_validation_source_view":
      return "keaMatchValidationRuleSourceView";
    case "match_validation_extraction":
      return "keaMatchValidationRuleExtraction";
    case "match_validation_aliasing":
      return "keaMatchValidationRuleAliasing";
    case "subflow":
      return "keaSubflow";
    case "subflow_graph_in":
      return "keaSubflowGraphIn";
    case "subflow_graph_out":
      return "keaSubflowGraphOut";
    case "subgraph":
      return "keaSubgraph";
    default:
      return "keaExtraction";
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
    case "keaExtraction":
      return "extraction";
    case "keaAliasing":
      return "aliasing";
    case "keaValidation":
      return "validation";
    case "keaAliasPersistence":
      return "alias_persistence";
    case "keaWritebackRaw":
      return "writeback_raw";
    case "keaWritebackDataModeling":
      return "writeback_data_modeling";
    case "keaReferenceIndex":
      return "reference_index";
    case "keaMatchValidationRuleSourceView":
      return "match_validation_source_view";
    case "keaMatchValidationRuleExtraction":
      return "match_validation_extraction";
    case "keaMatchValidationRuleAliasing":
      return "match_validation_aliasing";
    case "keaSubflow":
      return "subflow";
    case "keaSubflowGraphIn":
      return "subflow_graph_in";
    case "keaSubflowGraphOut":
      return "subflow_graph_out";
    case "keaSubgraph":
      return "subgraph";
    default:
      return "extraction";
  }
}
