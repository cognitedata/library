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

function hubInId(n: WorkflowCanvasNode): string | undefined {
  const v = n.data?.subflow_hub_input_id;
  return v != null && String(v).trim() ? String(v).trim() : undefined;
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
  const flattened = flattenSubgraphsForScopeSync(canvas);
  const byId = new Map(flattened.nodes.map((n) => [n.id, n]));
  let virtSeq = 0;
  const nextVirtId = (): string => `__scope_virt_${Date.now()}_${virtSeq++}`;

  const dropIds = new Set<string>();
  const add: WorkflowCanvasEdge[] = [];
  const addKeys = new Set<string>();

  const bridgeKey = (src: string, sh: string | null | undefined, tgt: string, th: string | null | undefined, kind: string) =>
    `${src}\0${sh ?? ""}\0${tgt}\0${th ?? ""}\0${kind}`;

  for (const e of flattened.edges) {
    if (!isBridgeableEdge(e)) {
      continue;
    }
    const tgt = byId.get(e.target);
    const src = byId.get(e.source);
    const th = e.target_handle != null ? String(e.target_handle) : "";
    const sh = e.source_handle != null ? String(e.source_handle) : "";
    if (tgt?.kind === "subflow" && th.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX)) {
      dropIds.add(e.id);
    }
    if (src?.kind === "subflow" && sh.startsWith(SUBFLOW_PORT_HANDLE_OUT_PREFIX)) {
      dropIds.add(e.id);
    }
    if (src?.kind === "subflow_graph_in") {
      dropIds.add(e.id);
    }
    if (tgt?.kind === "subflow_graph_out") {
      dropIds.add(e.id);
    }
  }

  for (const e of flattened.edges) {
    if (!isBridgeableEdge(e)) {
      continue;
    }
    const tgt = byId.get(e.target);
    if (!tgt || tgt.kind !== "subflow") {
      continue;
    }
    const th = e.target_handle != null ? String(e.target_handle) : "";
    if (!th.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX)) {
      continue;
    }
    const portId = parsePortIdFromSubflowTargetHandle(th);
    if (portId == null) {
      continue;
    }
    const hin = hubInId(tgt);
    if (!hin) {
      continue;
    }
    const wantHandle = subflowSourceHandleForPort(portId);
    const bridgeKind = isCompositionEdge(e) ? e.kind! : "data";
    for (const e2 of flattened.edges) {
      if (!isBridgeableEdge(e2)) {
        continue;
      }
      if (e2.source !== hin) {
        continue;
      }
      const sh2 = e2.source_handle != null ? String(e2.source_handle) : "";
      if (sh2 !== wantHandle) {
        continue;
      }
      const nk = bridgeKey(e.source, e.source_handle ?? null, e2.target, e2.target_handle ?? null, bridgeKind);
      if (addKeys.has(nk)) {
        continue;
      }
      addKeys.add(nk);
      add.push({
        id: nextVirtId(),
        source: e.source,
        target: e2.target,
        source_handle: e.source_handle ?? undefined,
        target_handle: e2.target_handle ?? undefined,
        kind: bridgeKind,
      });
    }
  }

  for (const e of flattened.edges) {
    if (!isBridgeableEdge(e)) {
      continue;
    }
    const tgt = byId.get(e.target);
    if (!tgt || tgt.kind !== "subflow_graph_out") {
      continue;
    }
    const th = e.target_handle != null ? String(e.target_handle) : "";
    const portId = parsePortIdFromSubflowTargetHandle(th);
    if (portId == null) {
      continue;
    }
    const sfId =
      tgt.parent_id != null && String(tgt.parent_id).trim() ? String(tgt.parent_id).trim() : "";
    const sf = sfId ? byId.get(sfId) : undefined;
    if (!sf || sf.kind !== "subflow") {
      continue;
    }
    const wantOut = subflowSourceHandleForPort(portId);
    const bridgeKind = isCompositionEdge(e) ? e.kind! : "data";
    for (const e2 of flattened.edges) {
      if (!isBridgeableEdge(e2)) {
        continue;
      }
      if (e2.source !== sf.id) {
        continue;
      }
      const sh2 = e2.source_handle != null ? String(e2.source_handle) : "";
      if (sh2 !== wantOut) {
        continue;
      }
      const nk = bridgeKey(e.source, e.source_handle ?? null, e2.target, e2.target_handle ?? null, bridgeKind);
      if (addKeys.has(nk)) {
        continue;
      }
      addKeys.add(nk);
      add.push({
        id: nextVirtId(),
        source: e.source,
        target: e2.target,
        source_handle: e.source_handle ?? undefined,
        target_handle: e2.target_handle ?? undefined,
        kind: bridgeKind,
      });
    }
  }

  const kept = flattened.edges.filter((e) => !dropIds.has(e.id));
  return { ...flattened, edges: [...kept, ...add] };
}
