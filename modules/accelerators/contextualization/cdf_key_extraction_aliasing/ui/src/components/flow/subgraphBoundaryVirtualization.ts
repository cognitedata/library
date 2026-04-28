import type { WorkflowCanvasDocument, WorkflowCanvasEdge, WorkflowCanvasNode } from "../../types/workflowCanvas";
import {
  SUBFLOW_PORT_HANDLE_IN_PREFIX,
  SUBFLOW_PORT_HANDLE_OUT_PREFIX,
  parsePortIdFromSubflowTargetHandle,
  subflowSourceHandleForPort,
} from "../../types/workflowCanvas";

function isDataEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind !== "sequence" && e.kind !== "parallel_group";
}

function hubInId(n: WorkflowCanvasNode): string | undefined {
  const v = n.data?.subflow_hub_input_id;
  return v != null && String(v).trim() ? String(v).trim() : undefined;
}

export function expandCanvasForScopeSync(canvas: WorkflowCanvasDocument): WorkflowCanvasDocument {
  const byId = new Map(canvas.nodes.map((n) => [n.id, n]));
  let virtSeq = 0;
  const nextVirtId = (): string => `__scope_virt_${Date.now()}_${virtSeq++}`;

  const dropIds = new Set<string>();
  const add: WorkflowCanvasEdge[] = [];
  const addKeys = new Set<string>();

  const bridgeKey = (src: string, sh: string | null | undefined, tgt: string, th: string | null | undefined) =>
    `${src}\0${sh ?? ""}\0${tgt}\0${th ?? ""}`;

  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
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

  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const tgt = byId.get(e.target);
    if (!tgt || tgt.kind !== "subflow") continue;
    const th = e.target_handle != null ? String(e.target_handle) : "";
    if (!th.startsWith(SUBFLOW_PORT_HANDLE_IN_PREFIX)) continue;
    const portId = parsePortIdFromSubflowTargetHandle(th);
    if (portId == null) continue;
    const hin = hubInId(tgt);
    if (!hin) continue;
    const wantHandle = subflowSourceHandleForPort(portId);
    for (const e2 of canvas.edges) {
      if (!isDataEdge(e2)) continue;
      if (e2.source !== hin) continue;
      const sh2 = e2.source_handle != null ? String(e2.source_handle) : "";
      if (sh2 !== wantHandle) continue;
      const nk = bridgeKey(e.source, e.source_handle ?? null, e2.target, e2.target_handle ?? null);
      if (addKeys.has(nk)) continue;
      addKeys.add(nk);
      add.push({
        id: nextVirtId(),
        source: e.source,
        target: e2.target,
        source_handle: e.source_handle ?? undefined,
        target_handle: e2.target_handle ?? undefined,
        kind: "data",
      });
    }
  }

  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const tgt = byId.get(e.target);
    if (!tgt || tgt.kind !== "subflow_graph_out") continue;
    const th = e.target_handle != null ? String(e.target_handle) : "";
    const portId = parsePortIdFromSubflowTargetHandle(th);
    if (portId == null) continue;
    const sfId =
      tgt.parent_id != null && String(tgt.parent_id).trim() ? String(tgt.parent_id).trim() : "";
    const sf = sfId ? byId.get(sfId) : undefined;
    if (!sf || sf.kind !== "subflow") continue;
    const wantOut = subflowSourceHandleForPort(portId);
    for (const e2 of canvas.edges) {
      if (!isDataEdge(e2)) continue;
      if (e2.source !== sf.id) continue;
      const sh2 = e2.source_handle != null ? String(e2.source_handle) : "";
      if (sh2 !== wantOut) continue;
      const nk = bridgeKey(e.source, e.source_handle ?? null, e2.target, e2.target_handle ?? null);
      if (addKeys.has(nk)) continue;
      addKeys.add(nk);
      add.push({
        id: nextVirtId(),
        source: e.source,
        target: e2.target,
        source_handle: e.source_handle ?? undefined,
        target_handle: e2.target_handle ?? undefined,
        kind: "data",
      });
    }
  }

  const kept = canvas.edges.filter((e) => !dropIds.has(e.id));
  return { ...canvas, edges: [...kept, ...add] };
}
