import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  cloneTransformFlowHistorySnapshot,
  pushTransformFlowHistory,
  TRANSFORM_FLOW_HISTORY_LIMIT,
  viewportToCanvasViewport,
} from "./transformFlowHistory";

const snap = {
  nodes: [{ id: "a", type: "etlTransform", position: { x: 1, y: 2 }, data: { label: "A" } }] as Node[],
  edges: [{ id: "e1", source: "a", target: "b" }] as Edge[],
  handleOrientation: "lr" as const,
  layoutMethod: "layered" as const,
  edgePathStyle: "smoothstep" as const,
  viewport: { x: 10, y: 20, zoom: 1.5 },
};

describe("transformFlowHistory", () => {
  it("clones snapshot deeply enough for undo", () => {
    const copy = cloneTransformFlowHistorySnapshot(snap);
    copy.nodes[0]!.position.x = 99;
    copy.viewport!.x = 0;
    expect(snap.nodes[0]!.position.x).toBe(1);
    expect(snap.viewport!.x).toBe(10);
  });

  it("caps history length", () => {
    let past: ReturnType<typeof cloneTransformFlowHistorySnapshot>[] = [];
    for (let i = 0; i < TRANSFORM_FLOW_HISTORY_LIMIT + 5; i++) {
      past = pushTransformFlowHistory(past, { ...snap, nodes: [{ ...snap.nodes[0]!, id: `n${i}` }] });
    }
    expect(past.length).toBe(TRANSFORM_FLOW_HISTORY_LIMIT);
  });

  it("maps viewport", () => {
    expect(viewportToCanvasViewport({ x: 1, y: 2, zoom: 0.5 })).toEqual({ x: 1, y: 2, zoom: 0.5 });
  });
});
