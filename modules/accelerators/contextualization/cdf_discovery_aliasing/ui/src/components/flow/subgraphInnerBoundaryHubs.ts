import type {
  SubflowPortsConfig,
  WorkflowCanvasDocument,
  WorkflowCanvasNode,
  WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import {
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
} from "../../types/workflowCanvas";
import { newNodeId } from "./flowDocumentBridge";

/** Default single in/out ports for a new or collapsed ``keaSubgraph`` boundary. */
export const DEFAULT_SUBGRAPH_FRAME_PORTS: SubflowPortsConfig = {
  inputs: [{ id: "in", label: "in" }],
  outputs: [{ id: "out", label: "out" }],
};

export const HUB_LANE_W = 136;
export const HUB_GAP = 24;
/** Vertical offset for inner boundary hubs (matches subflow wrap layout). */
export const SUBGRAPH_INNER_HEADER = 40;

export function subgraphFramePortsForInnerHubs(frame: SubflowPortsConfig): {
  hubInData: WorkflowCanvasNodeData;
  hubOutData: WorkflowCanvasNodeData;
} {
  return {
    hubInData: {
      label: "Graph inputs",
      subflow_ports: { inputs: frame.inputs ?? [], outputs: [] },
    },
    hubOutData: {
      label: "Graph outputs",
      subflow_ports: { inputs: [], outputs: frame.outputs ?? [] },
    },
  };
}

function canvasNodeBBox(nodes: WorkflowCanvasNode[]): { minX: number; maxX: number; minY: number; maxY: number } {
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const n of nodes) {
    if (n.kind === "subflow_graph_in" || n.kind === "subflow_graph_out") continue;
    const x = n.position?.x ?? 0;
    const y = n.position?.y ?? 0;
    const w = n.size?.width ?? 200;
    const h = n.size?.height ?? 80;
    minX = Math.min(minX, x);
    maxX = Math.max(maxX, x + w);
    minY = Math.min(minY, y);
    maxY = Math.max(maxY, y + h);
  }
  if (!Number.isFinite(minX)) {
    return { minX: 0, maxX: 240, minY: 0, maxY: 120 };
  }
  return { minX, maxX, minY, maxY };
}

/**
 * Ensure a subgraph ``inner_canvas`` document has Graph In / Graph Out hub nodes whose handles
 * mirror ``frame`` ports (standalone hubs — no ``parent_id`` to the outer ``keaSubgraph``).
 */
export function ensureSubgraphInnerBoundaryCanvasDocument(
  inner: WorkflowCanvasDocument,
  frame: SubflowPortsConfig,
  hubInIdHint?: string | null,
  hubOutIdHint?: string | null
): {
  doc: WorkflowCanvasDocument;
  hubInId: string;
  hubOutId: string;
  mutatedBoundaryMeta: boolean;
} {
  const inputs = frame.inputs?.length ? frame.inputs : [{ id: "in", label: "in" }];
  const outputs = frame.outputs?.length ? frame.outputs : [{ id: "out", label: "out" }];
  const frameNorm: SubflowPortsConfig = { inputs, outputs };
  const { hubInData, hubOutData } = subgraphFramePortsForInnerHubs(frameNorm);

  const nodes = [...inner.nodes];
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let hubInId = String(hubInIdHint ?? "").trim();
  let hubOutId = String(hubOutIdHint ?? "").trim();
  let mutatedBoundaryMeta = false;

  const validIn = hubInId && byId.get(hubInId)?.kind === "subflow_graph_in";
  const validOut = hubOutId && byId.get(hubOutId)?.kind === "subflow_graph_out";

  if (!validIn || !validOut) {
    hubInId = newNodeId();
    hubOutId = newNodeId();
    mutatedBoundaryMeta = true;
    const { minX, maxX, minY } = canvasNodeBBox(nodes);
    const inNode: WorkflowCanvasNode = {
      id: hubInId,
      kind: "subflow_graph_in",
      position: { x: minX - HUB_LANE_W - HUB_GAP, y: minY + SUBGRAPH_INNER_HEADER },
      data: { ...hubInData },
    };
    const outNode: WorkflowCanvasNode = {
      id: hubOutId,
      kind: "subflow_graph_out",
      position: { x: maxX + HUB_GAP, y: minY + SUBGRAPH_INNER_HEADER },
      data: { ...hubOutData },
    };
    nodes.push(inNode, outNode);
  } else {
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i]!;
      if (n.id === hubInId && n.kind === "subflow_graph_in") {
        nodes[i] = { ...n, data: { ...n.data, ...hubInData } };
      } else if (n.id === hubOutId && n.kind === "subflow_graph_out") {
        nodes[i] = { ...n, data: { ...n.data, ...hubOutData } };
      }
    }
  }

  return {
    doc: { ...inner, nodes },
    hubInId,
    hubOutId,
    mutatedBoundaryMeta,
  };
}

/** Recompute Graph In / Out hub positions from non-hub node bounds (after inner content changes). */
export function repositionSubgraphBoundaryHubsInDoc(
  inner: WorkflowCanvasDocument,
  hubInId: string,
  hubOutId: string
): WorkflowCanvasDocument {
  const { minX, maxX, minY } = canvasNodeBBox(inner.nodes);
  const nodes = inner.nodes.map((n) => {
    if (n.id === hubInId && n.kind === "subflow_graph_in") {
      return {
        ...n,
        position: { x: minX - HUB_LANE_W - HUB_GAP, y: minY + SUBGRAPH_INNER_HEADER },
      };
    }
    if (n.id === hubOutId && n.kind === "subflow_graph_out") {
      return {
        ...n,
        position: { x: maxX + HUB_GAP, y: minY + SUBGRAPH_INNER_HEADER },
      };
    }
    return n;
  });
  return { ...inner, nodes };
}

/** Push updated frame ports onto inner hub node ``data`` (after inspector port edits). */
export function syncSubgraphInnerHubPortData(
  inner: WorkflowCanvasDocument,
  hubInId: string,
  hubOutId: string,
  frame: SubflowPortsConfig
): WorkflowCanvasDocument {
  const inputs = frame.inputs?.length ? frame.inputs : [{ id: "in", label: "in" }];
  const outputs = frame.outputs?.length ? frame.outputs : [{ id: "out", label: "out" }];
  const { hubInData, hubOutData } = subgraphFramePortsForInnerHubs({ inputs, outputs });
  const nodes = inner.nodes.map((n) => {
    if (n.id === hubInId && n.kind === "subflow_graph_in") {
      return { ...n, data: { ...n.data, ...hubInData } };
    }
    if (n.id === hubOutId && n.kind === "subflow_graph_out") {
      return { ...n, data: { ...n.data, ...hubOutData } };
    }
    return n;
  });
  return { ...inner, nodes };
}

/** Remove inner edges that referenced subgraph frame ports which were deleted in the inspector. */
export function pruneSubgraphInnerPortEdges(
  inner: WorkflowCanvasDocument,
  hubInId: string,
  hubOutId: string,
  removedIn: string[],
  removedOut: string[]
): WorkflowCanvasDocument {
  if (removedIn.length === 0 && removedOut.length === 0) return inner;
  const edges = inner.edges.filter((e) => {
    if (e.source === hubInId) {
      const pid = parsePortIdFromSubflowSourceHandle(e.source_handle ?? undefined);
      if (pid && removedIn.includes(pid)) return false;
    }
    if (e.target === hubOutId) {
      const pid = parsePortIdFromSubflowTargetHandle(e.target_handle ?? undefined);
      if (pid && removedOut.includes(pid)) return false;
    }
    return true;
  });
  return { ...inner, edges };
}
