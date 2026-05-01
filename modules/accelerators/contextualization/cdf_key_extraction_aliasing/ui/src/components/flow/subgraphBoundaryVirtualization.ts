import type { WorkflowCanvasDocument, WorkflowCanvasEdge, WorkflowCanvasNode } from "../../types/workflowCanvas";
import {
  SUBFLOW_PORT_HANDLE_IN_PREFIX,
  SUBFLOW_PORT_HANDLE_OUT_PREFIX,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
} from "../../types/workflowCanvas";

function isDataEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind !== "sequence" && e.kind !== "parallel_group";
}

function isCompositionEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind === "sequence" || e.kind === "parallel_group";
}

function isBridgeableEdge(e: WorkflowCanvasEdge): boolean {
  return isDataEdge(e) || isCompositionEdge(e);
}

function slugSg(s: string): string {
  const t = s.replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_|_$/g, "");
  return t || "sg";
}

let liftSeq = 0;
const nextLiftEdgeId = (): string => `__scope_lift_e_${Date.now()}_${liftSeq++}`;

/**
 * Hoist ``kind: subgraph`` ``inner_canvas`` nodes to the top-level document so scope sync sees the
 * same logical graph as compile (ids prefixed to avoid collisions). Hub in/out nodes are stripped.
 */
export function flattenSubgraphsForScopeSync(doc: WorkflowCanvasDocument): WorkflowCanvasDocument {
  let cur = doc;
  for (let guard = 0; guard < 32; guard++) {
    const sg = cur.nodes.find(
      (n) => n.kind === "subgraph" && n.data.inner_canvas?.nodes && n.data.inner_canvas.nodes.length > 0
    );
    if (!sg) {
      break;
    }
    cur = liftOneSubgraphFromDoc(cur, sg);
  }
  return cur;
}

function liftOneSubgraphFromDoc(doc: WorkflowCanvasDocument, sg: WorkflowCanvasNode): WorkflowCanvasDocument {
  const inner = sg.data.inner_canvas!;
  let hubInId = String(sg.data.subflow_hub_input_id ?? "").trim();
  let hubOutId = String(sg.data.subflow_hub_output_id ?? "").trim();
  if (!hubInId) {
    hubInId = inner.nodes.find((n) => n.kind === "subflow_graph_in")?.id ?? "";
  }
  if (!hubOutId) {
    hubOutId = inner.nodes.find((n) => n.kind === "subflow_graph_out")?.id ?? "";
  }
  const prefix = `__sg_${slugSg(sg.id)}__`;
  const outerIds = new Set(doc.nodes.map((n) => n.id));
  outerIds.delete(sg.id);

  const innerContentNodes = inner.nodes.filter(
    (n) =>
      n.id !== hubInId &&
      n.id !== hubOutId &&
      n.kind !== "subflow_graph_in" &&
      n.kind !== "subflow_graph_out"
  );
  const idRemap = new Map<string, string>();
  for (const n of innerContentNodes) {
    const newId = `${prefix}${n.id}`;
    idRemap.set(n.id, newId);
  }

  const remappedNodes: WorkflowCanvasNode[] = innerContentNodes.map((n) => ({
    ...n,
    id: idRemap.get(n.id)!,
    parent_id: null,
  }));

  const mapEnd = (id: string): string => idRemap.get(id) ?? id;
  const newEdges: WorkflowCanvasEdge[] = [];

  for (const e of doc.edges) {
    if (e.target === sg.id) {
      const th = e.target_handle != null ? String(e.target_handle) : "";
      if (!th.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX)) {
        newEdges.push(e);
        continue;
      }
      const portId = parsePortIdFromSubflowTargetHandle(th);
      if (portId == null) {
        continue;
      }
      const wantHandle = subflowSourceHandleForPort(portId);
      for (const ie of inner.edges ?? []) {
        if (!isBridgeableEdge(ie)) {
          continue;
        }
        if (ie.source !== hubInId) {
          continue;
        }
        const sh2 = ie.source_handle != null ? String(ie.source_handle) : "";
        if (sh2 !== wantHandle) {
          continue;
        }
        const bridgeKind = isCompositionEdge(e) ? e.kind! : "data";
        newEdges.push({
          ...e,
          id: nextLiftEdgeId(),
          target: mapEnd(ie.target),
          target_handle: ie.target_handle ?? undefined,
          kind: bridgeKind,
        });
      }
      continue;
    }
    if (e.source === sg.id) {
      const sh = e.source_handle != null ? String(e.source_handle) : "";
      if (!sh.startsWith(SUBFLOW_PORT_HANDLE_OUT_PREFIX)) {
        newEdges.push(e);
        continue;
      }
      const portId = parsePortIdFromSubflowSourceHandle(sh);
      if (portId == null) {
        continue;
      }
      const wantIn = subflowTargetHandleForPort(portId);
      for (const ie of inner.edges ?? []) {
        if (!isBridgeableEdge(ie)) {
          continue;
        }
        if (ie.target !== hubOutId) {
          continue;
        }
        const th2 = ie.target_handle != null ? String(ie.target_handle) : "";
        if (th2 !== wantIn) {
          continue;
        }
        const bridgeKind = isCompositionEdge(e) ? e.kind! : "data";
        newEdges.push({
          ...e,
          id: nextLiftEdgeId(),
          source: mapEnd(ie.source),
          source_handle: ie.source_handle ?? undefined,
          kind: bridgeKind,
        });
      }
      continue;
    }
    if (e.source === sg.id || e.target === sg.id) {
      continue;
    }
    newEdges.push(e);
  }

  for (const ie of inner.edges ?? []) {
    if (ie.source === hubInId || ie.target === hubOutId) {
      continue;
    }
    if (!idRemap.has(ie.source) || !idRemap.has(ie.target)) {
      continue;
    }
    newEdges.push({
      ...ie,
      id: nextLiftEdgeId(),
      source: mapEnd(ie.source),
      target: mapEnd(ie.target),
    });
  }

  const newNodes = doc.nodes.filter((n) => n.id !== sg.id).concat(remappedNodes);
  return { ...doc, nodes: newNodes, edges: newEdges };
}

export function expandCanvasForScopeSync(canvas: WorkflowCanvasDocument): WorkflowCanvasDocument {
  return flattenSubgraphsForScopeSync(canvas);
}
