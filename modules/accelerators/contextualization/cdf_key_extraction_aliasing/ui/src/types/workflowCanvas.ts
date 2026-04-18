/**
 * workflow.local.canvas.yaml / workflow.template.canvas.yaml — layout-only document
 * paired with the scope document (canonical config remains in workflow *.config.yaml).
 */

export const WORKFLOW_CANVAS_SCHEMA_VERSION = 1;

export type CanvasNodeRfType =
  | "keaStart"
  | "keaEnd"
  | "keaSourceView"
  | "keaExtraction"
  | "keaAliasing"
  | "keaValidation"
  | "keaAliasPersistence"
  | "keaReferenceIndex"
  | "keaMatchValidationRuleSourceView"
  | "keaMatchValidationRuleExtraction"
  | "keaMatchValidationRuleAliasing";

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
  | "reference_index"
  | "match_validation_source_view"
  | "match_validation_extraction"
  | "match_validation_aliasing";

export interface CanvasNodeRef {
  /** Index into source_views[] */
  source_view_index?: number;
  /** Match key for source view */
  view_space?: string;
  view_external_id?: string;
  view_version?: string;
  entity_type?: string;
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
  /** fn_dm_incremental_state_update — cohort rows before key extraction when incremental is on */
  incremental_step?: "state_update";
  /** e.g. regex_handler, heuristic, character_substitution */
  handler_id?: string;
  /** True when dropped from a preset palette item */
  preset_from_palette?: boolean;
  ref?: CanvasNodeRef;
  notes?: string;
  /** validation / annotation sub-kind (layout overlays — not confidence_match_rules) */
  annotation_kind?: "global_validation" | "edge_validation";
  /**
   * Confidence-match validation rule (`confidence_match_rules[]`) — where it is evaluated in scope YAML.
   */
  validation_rule_context?: "source_view" | "extraction" | "aliasing";
  /** Name of the `confidence_match_rules` entry (paired with ref.* parent). */
  confidence_match_rule_name?: string;
}

export interface WorkflowCanvasNode {
  id: string;
  kind: CanvasNodeKind;
  position: { x: number; y: number };
  data: WorkflowCanvasNodeData;
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
}

export function emptyWorkflowCanvasDocument(): WorkflowCanvasDocument {
  return {
    schemaVersion: WORKFLOW_CANVAS_SCHEMA_VERSION,
    nodes: [],
    edges: [],
  };
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
        kind !== "reference_index" &&
        kind !== "match_validation_source_view" &&
        kind !== "match_validation_extraction" &&
        kind !== "match_validation_aliasing"
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
      nodes.push({ id, kind, position, data });
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
    if (
      src?.kind === "extraction" &&
      e.source_handle &&
      String(e.source_handle).startsWith("out_")
    ) {
      return { ...e, source_handle: null };
    }
    return e;
  });
  return { schemaVersion, nodes, edges: edgesNormalized };
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
    case "reference_index":
      return "keaReferenceIndex";
    case "match_validation_source_view":
      return "keaMatchValidationRuleSourceView";
    case "match_validation_extraction":
      return "keaMatchValidationRuleExtraction";
    case "match_validation_aliasing":
      return "keaMatchValidationRuleAliasing";
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
    case "keaReferenceIndex":
      return "reference_index";
    case "keaMatchValidationRuleSourceView":
      return "match_validation_source_view";
    case "keaMatchValidationRuleExtraction":
      return "match_validation_extraction";
    case "keaMatchValidationRuleAliasing":
      return "match_validation_aliasing";
    default:
      return "extraction";
  }
}
