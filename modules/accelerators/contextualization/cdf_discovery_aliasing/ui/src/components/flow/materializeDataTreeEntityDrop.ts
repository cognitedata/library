import type { Edge, Node } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import { createNodeFromPalette } from "./createNodeFromPalette";
import type { DataTreeEntityDragPayload } from "../../types/dataTree";
import { discoveryFlowEdgeVisualDefaults } from "./flowDocumentBridge";
import {
  buildPersistenceOutboundToEndDataEdge,
  findFirstDiscoveryEndNodeId,
} from "./flowEdgeHelpers";
import { discoveryPersistenceOutboundToEndOnlyRfTypes } from "./flowConstants";
import { entityDropStages, seedConfigForEntityDrop } from "../../utils/dataTreeEntityDrop";
import { newNodeId } from "./flowDocumentBridge";

const SAVE_OFFSET_Y = 140;

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export type MaterializeDataTreeEntityDropInput = {
  payload: DataTreeEntityDragPayload;
  position: { x: number; y: number };
  nodes: Node[];
  schemaSpace?: string;
  t: TFn;
  /** When inserting on an existing edge, omit Start→query auto-wire (edge split supplies upstream). */
  skipStartToQueryWire?: boolean;
};

export type MaterializeDataTreeEntityDropResult = {
  nodes: Node[];
  extraEdges: Edge[];
  selectNodeId: string;
};

function patchNodeConfig(node: Node, config: Record<string, unknown>): Node {
  const data = (node.data ?? {}) as Record<string, unknown>;
  return {
    ...node,
    data: {
      ...data,
      label: String(config.description ?? data.label ?? ""),
      config,
    },
  };
}

export function materializeDataTreeEntityDrop(
  input: MaterializeDataTreeEntityDropInput
): MaterializeDataTreeEntityDropResult | null {
  const { payload, position, nodes, schemaSpace, t } = input;
  const stages = entityDropStages(payload.node);
  if (!stages) return null;

  const queryConfig = seedConfigForEntityDrop(payload.node, stages.query, schemaSpace);
  const saveConfig = seedConfigForEntityDrop(payload.node, stages.save, schemaSpace);

  let queryNode = createNodeFromPalette(
    { kind: "discovery", stage: stages.query },
    position,
    { t }
  );
  queryNode = patchNodeConfig(queryNode, queryConfig);

  let saveNode = createNodeFromPalette(
    { kind: "discovery", stage: stages.save },
    { x: position.x, y: position.y + SAVE_OFFSET_Y },
    { t }
  );
  saveNode = patchNodeConfig(saveNode, {
    ...saveConfig,
    save_fan_in_mode: "none",
  });

  const extraEdges: Edge[] = [];
  const startId =
    input.skipStartToQueryWire === true
      ? undefined
      : nodes.find((n) => n.type === "discoveryStart")?.id;
  if (startId) {
    extraEdges.push({
      ...discoveryFlowEdgeVisualDefaults,
      id: `e_${startId}_${queryNode.id}_${Date.now()}`,
      source: startId,
      sourceHandle: "out",
      target: queryNode.id,
      targetHandle: "in",
      data: { kind: "data" },
    });
  }

  extraEdges.push({
    ...discoveryFlowEdgeVisualDefaults,
    id: `e_${queryNode.id}_${saveNode.id}_${Date.now()}`,
    source: queryNode.id,
    sourceHandle: "out",
    target: saveNode.id,
    targetHandle: "in",
    data: { kind: "data" },
  });

  const rfType = saveNode.type ?? "";
  if (discoveryPersistenceOutboundToEndOnlyRfTypes.has(rfType)) {
    const endId = findFirstDiscoveryEndNodeId(nodes);
    if (endId) {
      extraEdges.push(buildPersistenceOutboundToEndDataEdge(saveNode.id, endId));
    }
  }

  return {
    nodes: [queryNode, saveNode],
    extraEdges,
    selectNodeId: queryNode.id,
  };
}

/** Assign unique ids when batch-creating (createNodeFromPalette already uses newNodeId per call). */
export function ensureUniqueBatchNodeIds(nodes: Node[]): Node[] {
  const seen = new Set<string>();
  return nodes.map((n) => {
    let id = n.id;
    while (seen.has(id)) {
      id = newNodeId();
    }
    seen.add(id);
    return id === n.id ? n : { ...n, id };
  });
}
