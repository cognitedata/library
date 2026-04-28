import type { Edge, Node } from "@xyflow/react";
import {
  type SubflowPortEntry,
  type SubflowPortsConfig,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { newNodeId } from "./flowDocumentBridge";

function slugPart(s: string): string {
  return String(s).replace(/[^a-zA-Z0-9_]/g, "_").replace(/_+/g, "_").slice(0, 48) || "x";
}

/** Crossing edge: distinct key for the member node's inbound socket (who receives from outside). */
export function memberInboundPortKey(e: Edge): string {
  return `${e.target}\0${e.targetHandle ?? ""}`;
}

/** Crossing edge: distinct key for the member node's outbound socket (who sends to outside). */
export function memberOutboundPortKey(e: Edge): string {
  return `${e.source}\0${e.sourceHandle ?? ""}`;
}

function labelForSource(nodes: Node[], sourceId: string): string {
  const n = nodes.find((x) => x.id === sourceId);
  const d = (n?.data ?? {}) as WorkflowCanvasNodeData;
  const lab = d.label;
  if (lab != null && String(lab).trim()) return String(lab).trim();
  return sourceId;
}

/**
 * Crossing edges from outside the member set into members (optionally excluding edges whose
 * source is ``excludeSourceId``, e.g. the subgraph node when adopting into an existing subgraph).
 */
export function crossingExternalToMemberEdges(
  edges: Edge[],
  memberIds: Set<string>,
  excludeSourceId?: string
): Edge[] {
  return edges.filter(
    (e) =>
      !memberIds.has(e.source) &&
      memberIds.has(e.target) &&
      (!excludeSourceId || e.source !== excludeSourceId)
  );
}

/** Member → outside (optionally excluding edges into ``excludeTargetId``, e.g. the subgraph node). */
export function crossingMemberToExternalEdges(
  edges: Edge[],
  memberIds: Set<string>,
  excludeTargetId?: string
): Edge[] {
  return edges.filter(
    (e) =>
      memberIds.has(e.source) &&
      !memberIds.has(e.target) &&
      (!excludeTargetId || e.target !== excludeTargetId)
  );
}

function labelForTarget(nodes: Node[], targetId: string): string {
  const n = nodes.find((x) => x.id === targetId);
  const d = (n?.data ?? {}) as WorkflowCanvasNodeData;
  const lab = d.label;
  if (lab != null && String(lab).trim()) return String(lab).trim();
  return targetId;
}

/**
 * One subgraph input port per distinct **inner** inbound socket (member target node + target handle)
 * that receives an edge from outside the member set. Port labels use the inner node's display label.
 */
export function buildMergedInputPortsForMemberCrossings(
  nodes: Node[],
  edges: Edge[],
  memberIds: Set<string>,
  existingFrame: SubflowPortsConfig,
  excludeSourceId?: string
): { frame: SubflowPortsConfig; keyToPortId: Map<string, string> } {
  const crossing = crossingExternalToMemberEdges(edges, memberIds, excludeSourceId);
  const keys = [...new Set(crossing.map(memberInboundPortKey))].sort((a, b) => a.localeCompare(b));
  const keyToPortId = new Map<string, string>();
  if (keys.length === 0) {
    return { frame: existingFrame, keyToPortId };
  }

  const defIn =
    (existingFrame.inputs?.length ?? 0) === 1 && existingFrame.inputs![0]!.id === "in";
  const defOut =
    (existingFrame.outputs?.length ?? 0) === 1 && existingFrame.outputs![0]!.id === "out";
  const isDefaultShell = defIn && defOut;

  const used = new Set<string>();
  /** Replacing the whole default ``in``/``out`` shell — do not seed ``used`` with ``in`` or we cannot reuse ``in``. */
  const replacingDefaultInputShell = isDefaultShell && keys.length > 0;
  if (!replacingDefaultInputShell) {
    for (const p of existingFrame.inputs ?? []) used.add(p.id);
  }

  const newInputEntries: SubflowPortEntry[] = [];
  for (let idx = 0; idx < keys.length; idx++) {
    const k = keys[idx]!;
    const [tgtId, tgtHandle] = k.split("\0");
    let pid: string;
    if (isDefaultShell && keys.length === 1) {
      pid = "in";
    } else if (isDefaultShell && keys.length > 1) {
      pid = idx === 0 ? "in" : `in_${slugPart(tgtId)}_${slugPart(tgtHandle)}`;
    } else {
      pid = `in_${slugPart(tgtId)}_${slugPart(tgtHandle)}`;
    }
    while (used.has(pid)) {
      pid = `${pid}_${newNodeId().slice(-6)}`;
    }
    used.add(pid);
    keyToPortId.set(k, pid);
    const innerTargetRfType = nodes.find((x) => x.id === tgtId)?.type;
    newInputEntries.push({
      id: pid,
      label: labelForTarget(nodes, tgtId),
      ...(innerTargetRfType ? { inner_target_rf_type: innerTargetRfType } : {}),
    });
  }

  const nextInputs =
    isDefaultShell && keys.length > 0
      ? newInputEntries
      : (() => {
          const merged = [...(existingFrame.inputs ?? [])];
          for (const ent of newInputEntries) {
            if (!merged.some((x) => x.id === ent.id)) merged.push(ent);
          }
          return merged;
        })();

  const frame: SubflowPortsConfig = {
    inputs: nextInputs,
    outputs: [...(existingFrame.outputs ?? [])],
  };
  return { frame, keyToPortId };
}

/**
 * One subgraph output port per distinct **inner** outbound socket (member source node + source handle)
 * that sends an edge to outside the member set. Port labels use the inner node's display label.
 */
export function buildMergedOutputPortsForMemberCrossings(
  nodes: Node[],
  edges: Edge[],
  memberIds: Set<string>,
  frameAfterInputs: SubflowPortsConfig,
  excludeTargetId?: string
): { frame: SubflowPortsConfig; outputKeyToPortId: Map<string, string> } {
  const crossing = crossingMemberToExternalEdges(edges, memberIds, excludeTargetId);
  const keys = [...new Set(crossing.map(memberOutboundPortKey))].sort((a, b) => a.localeCompare(b));
  const outputKeyToPortId = new Map<string, string>();
  if (keys.length === 0) {
    return { frame: frameAfterInputs, outputKeyToPortId };
  }

  const defOutOnly =
    (frameAfterInputs.outputs?.length ?? 0) === 1 && frameAfterInputs.outputs![0]!.id === "out";

  const used = new Set<string>();
  for (const p of frameAfterInputs.inputs ?? []) used.add(p.id);
  const replacingDefaultOutputShell = defOutOnly && keys.length > 0;
  if (!replacingDefaultOutputShell) {
    for (const p of frameAfterInputs.outputs ?? []) used.add(p.id);
  }

  const newOutputEntries: SubflowPortEntry[] = [];
  for (let idx = 0; idx < keys.length; idx++) {
    const k = keys[idx]!;
    const [srcId, srcHandle] = k.split("\0");
    let pid: string;
    if (defOutOnly && keys.length === 1) {
      pid = "out";
    } else if (defOutOnly && keys.length > 1) {
      pid = idx === 0 ? "out" : `out_${slugPart(srcId)}_${slugPart(srcHandle)}`;
    } else {
      pid = `out_${slugPart(srcId)}_${slugPart(srcHandle)}`;
    }
    while (used.has(pid)) {
      pid = `${pid}_${newNodeId().slice(-6)}`;
    }
    used.add(pid);
    outputKeyToPortId.set(k, pid);
    const innerSourceRfType = nodes.find((x) => x.id === srcId)?.type;
    newOutputEntries.push({
      id: pid,
      label: labelForSource(nodes, srcId),
      ...(innerSourceRfType ? { inner_source_rf_type: innerSourceRfType } : {}),
    });
  }

  const nextOutputs =
    replacingDefaultOutputShell && keys.length > 0
      ? newOutputEntries
      : (() => {
          const merged = [...(frameAfterInputs.outputs ?? [])];
          for (const ent of newOutputEntries) {
            if (!merged.some((x) => x.id === ent.id)) merged.push(ent);
          }
          return merged;
        })();

  const frame: SubflowPortsConfig = {
    inputs: [...(frameAfterInputs.inputs ?? [])],
    outputs: nextOutputs,
  };
  return { frame, outputKeyToPortId };
}
