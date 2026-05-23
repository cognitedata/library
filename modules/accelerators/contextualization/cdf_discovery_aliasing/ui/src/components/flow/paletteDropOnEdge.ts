import type { Connection, Edge, Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import type { CompileWorkflowDagMode } from "../../utils/workflowCompileMode";
import type { PaletteDropPayload } from "./FlowPalette";
import { getPaletteDropPayload } from "./FlowPalette";
import { appendDiscoveryConnectionEdge, appendReuseDataEdge, dedupeEdgesByHandles } from "./flowEdgeHelpers";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { discoveryValidationRuleLayoutRfTypes } from "./flowConstants";
import { isValidDiscoveryFlowConnection } from "./subgraphFlowConnections";
import { materializeDataTreeEntityDrop } from "./materializeDataTreeEntityDrop";
import { materializePaletteDrop, type MaterializePaletteDropInput } from "./materializePaletteDrop";
import type { PaletteDragPayload } from "./FlowPalette";
import { appendNodeAndResolveSubflowParent } from "./subflowDropAssociation";

type GetNode = (id: string) => Node | undefined;
type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

const HIT_THRESHOLD_BASE = 22;

function nodeCenter(n: Node): { x: number; y: number } {
  const w = n.measured?.width ?? n.width ?? 160;
  const h = n.measured?.height ?? n.height ?? 48;
  return { x: n.position.x + w / 2, y: n.position.y + h / 2 };
}

function distancePointToSegment(
  p: { x: number; y: number },
  a: { x: number; y: number },
  b: { x: number; y: number }
): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lenSq = dx * dx + dy * dy;
  if (lenSq < 1e-6) {
    return Math.hypot(p.x - a.x, p.y - a.y);
  }
  const t = Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / lenSq));
  const px = a.x + t * dx;
  const py = a.y + t * dy;
  return Math.hypot(p.x - px, p.y - py);
}

/** Primary data edges that may accept an inserted palette node. */
export function isSplittableDataEdge(edge: Edge): boolean {
  const kind = (edge.data as FlowEdgeData | undefined)?.kind ?? "data";
  if (kind !== "data") return false;
  const sh = edge.sourceHandle ?? "out";
  if (sh === "validation") return false;
  return true;
}

export function findEdgeAtFlowPoint(
  flowPoint: { x: number; y: number },
  edges: Edge[],
  getNode: GetNode,
  zoom = 1
): Edge | null {
  const threshold = HIT_THRESHOLD_BASE / Math.max(zoom, 0.25);
  let best: { edge: Edge; dist: number } | null = null;
  for (const edge of edges) {
    if (!isSplittableDataEdge(edge)) continue;
    const src = getNode(edge.source);
    const tgt = getNode(edge.target);
    if (!src || !tgt) continue;
    const dist = distancePointToSegment(flowPoint, nodeCenter(src), nodeCenter(tgt));
    if (dist > threshold) continue;
    if (!best || dist < best.dist) best = { edge, dist };
  }
  return best?.edge ?? null;
}

function getNodeWithSyntheticTypes(getNode: GetNode, synthetic: Map<string, string>): GetNode {
  return (id: string) => {
    const rf = synthetic.get(id);
    if (rf) {
      return { id, type: rf, position: { x: 0, y: 0 }, data: {} } as Node;
    }
    return getNode(id);
  };
}

export function canInsertNodesOnEdge(
  edge: Edge,
  segments: Array<{ nodeId: string; rfType: string }>,
  getNode: GetNode,
  compileDagMode: CompileWorkflowDagMode = "canvas"
): boolean {
  if (!isSplittableDataEdge(edge) || segments.length === 0) return false;
  const synthetic = new Map(segments.map((s) => [s.nodeId, s.rfType]));
  const resolve = getNodeWithSyntheticTypes(getNode, synthetic);

  let prevSource = edge.source;
  let prevSourceHandle = edge.sourceHandle ?? "out";
  for (const seg of segments) {
    const conn: Connection = {
      source: prevSource,
      sourceHandle: prevSourceHandle,
      target: seg.nodeId,
      targetHandle: "in",
    };
    if (!isValidDiscoveryFlowConnection(resolve, conn, discoveryValidationRuleLayoutRfTypes, compileDagMode)) {
      return false;
    }
    prevSource = seg.nodeId;
    prevSourceHandle = "out";
  }
  const tailConn: Connection = {
    source: prevSource,
    sourceHandle: prevSourceHandle,
    target: edge.target,
    targetHandle: edge.targetHandle ?? "in",
  };
  return isValidDiscoveryFlowConnection(resolve, tailConn, discoveryValidationRuleLayoutRfTypes, compileDagMode);
}

export function replaceEdgeWithInsertedChain(
  getNode: GetNode,
  edges: Edge[],
  edge: Edge,
  chainNodeIds: string[],
  extraEdges: Edge[] = []
): Edge[] {
  if (chainNodeIds.length === 0) return edges;
  const without = edges.filter((e) => e.id !== edge.id);
  const ids = [edge.source, ...chainNodeIds, edge.target];
  let merged = without;
  for (let i = 0; i < ids.length - 1; i++) {
    const conn: Connection = {
      source: ids[i]!,
      sourceHandle: i === 0 ? edge.sourceHandle ?? "out" : "out",
      target: ids[i + 1]!,
      targetHandle: i === ids.length - 2 ? edge.targetHandle ?? "in" : "in",
    };
    merged = appendDiscoveryConnectionEdge(getNode, merged, conn);
  }
  return dedupeEdgesByHandles([...merged, ...extraEdges]);
}

function filterExtraEdgesForEdgeSplit(
  extraEdges: Edge[],
  hitEdge: Edge,
  chainIds: string[],
  nodes: readonly Node[]
): Edge[] {
  const chainSet = new Set(chainIds);
  const startId = nodes.find((n) => n.type === "discoveryStart")?.id;
  return extraEdges.filter((e) => {
    if (e.source === hitEdge.source && chainSet.has(e.target)) return false;
    if (chainSet.has(e.source) && e.target === hitEdge.target) return false;
    if (startId && e.source === startId && chainSet.has(e.target)) return false;
    return true;
  });
}

export type ApplyPaletteCanvasDropInput = {
  event: React.DragEvent;
  screenToFlowPosition: (pos: { x: number; y: number }) => { x: number; y: number };
  getNode: GetNode;
  getEdges: () => Edge[];
  zoom?: number;
  nodes: Node[];
  edges: Edge[];
  workflowScopeDoc: Record<string, unknown>;
  patchWorkflowScope: MaterializePaletteDropInput["patchWorkflowScope"];
  t: TFn;
  schema_space?: string;
  compileDagMode?: CompileWorkflowDagMode;
  allowValidationRuleLayoutReuse?: boolean;
  setNodes: (recipe: (nds: Node[]) => Node[]) => void;
  setEdges: (recipe: (eds: Edge[]) => Edge[]) => void;
  onSelectNodeId: (nodeId: string) => void;
};

/** Apply palette or CDF Data tree drop; returns true when the drop was handled. */
export function applyPaletteCanvasDrop(input: ApplyPaletteCanvasDropInput): boolean {
  const {
    event,
    screenToFlowPosition,
    getNode,
    getEdges,
    zoom = 1,
    nodes,
    edges,
    workflowScopeDoc,
    patchWorkflowScope,
    t,
    schema_space,
    compileDagMode = "canvas",
    allowValidationRuleLayoutReuse = true,
    setNodes,
    setEdges,
    onSelectNodeId,
  } = input;

  const payload = getPaletteDropPayload(event);
  if (!payload) return false;

  const flowPosition = screenToFlowPosition({ x: event.clientX, y: event.clientY });
  const candidateEdge = findEdgeAtFlowPoint(flowPosition, getEdges(), getNode, zoom);

  if (payload.kind === "data_tree_entity") {
    const batch = materializeDataTreeEntityDrop({
      payload,
      position: flowPosition,
      nodes,
      schema_space,
      t,
    });
    if (!batch) return false;

    const segments = batch.nodes.map((n) => ({
      nodeId: n.id,
      rfType: n.type ?? "",
    }));

    const hitEdge =
      candidateEdge && canInsertNodesOnEdge(candidateEdge, segments, getNode, compileDagMode)
        ? candidateEdge
        : null;

    if (hitEdge) {
      const chainIds = segments.map((s) => s.nodeId);
      const extra = filterExtraEdgesForEdgeSplit(batch.extraEdges, hitEdge, chainIds, nodes);
      setNodes((nds) => {
        let next = nds;
        for (const node of batch.nodes) {
          next = appendNodeAndResolveSubflowParent(next, node);
        }
        return next;
      });
      setEdges((eds) => replaceEdgeWithInsertedChain(getNode, eds, hitEdge, chainIds, extra));
      onSelectNodeId(batch.selectNodeId);
      return true;
    }

    setNodes((nds) => {
      let next = nds;
      for (const node of batch.nodes) {
        next = appendNodeAndResolveSubflowParent(next, node);
      }
      return next;
    });
    setEdges((eds) => [...eds, ...batch.extraEdges]);
    onSelectNodeId(batch.selectNodeId);
    return true;
  }

  const palettePayload = payload as PaletteDragPayload;
  const result = materializePaletteDrop({
    payload: palettePayload,
    position: flowPosition,
    nodes,
    edges,
    workflowScopeDoc,
    patchWorkflowScope,
    t,
    allowValidationRuleLayoutReuse,
  });

  if (result.outcome === "reuse") {
    const head = getNode(result.headId);
    const rf = head?.type ?? "";
    const hitEdge =
      candidateEdge &&
      head &&
      canInsertNodesOnEdge(candidateEdge, [{ nodeId: result.headId, rfType: rf }], getNode, compileDagMode)
        ? candidateEdge
        : null;
    if (hitEdge) {
      setEdges((eds) =>
        replaceEdgeWithInsertedChain(getNode, eds, hitEdge, [result.headId], [])
      );
      onSelectNodeId(result.headId);
      return true;
    }
    const connectFromId = result.connectFromId;
    if (connectFromId) {
      setEdges((eds) => appendReuseDataEdge(eds, connectFromId, result.headId));
    }
    onSelectNodeId(result.headId);
    return true;
  }

  const { node, extraEdges } = result;
  const rf = node.type ?? "";

  const hitEdge =
    candidateEdge &&
    canInsertNodesOnEdge(candidateEdge, [{ nodeId: node.id, rfType: rf }], getNode, compileDagMode)
      ? candidateEdge
      : null;

  if (hitEdge) {
    const extra = filterExtraEdgesForEdgeSplit(extraEdges, hitEdge, [node.id], nodes);
    setNodes((nds) => appendNodeAndResolveSubflowParent(nds, node));
    setEdges((eds) => replaceEdgeWithInsertedChain(getNode, eds, hitEdge, [node.id], extra));
    onSelectNodeId(node.id);
    return true;
  }

  setNodes((nds) => appendNodeAndResolveSubflowParent(nds, node));
  setEdges((eds) => [...eds, ...extraEdges]);
  if (palettePayload.kind === "structural" && palettePayload.nodeKind === "source_view") {
    onSelectNodeId(node.id);
  }
  return true;
}
