import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { cloneFlowLayoutHistorySnapshot, pushFlowLayoutHistory } from "../transform/transformFlowHistory";

const node = (id: string, x: number): Node => ({
  id,
  type: "wfTask",
  position: { x, y: 0 },
  data: { task: { id } },
});

describe("flow layout history", () => {
  it("clones node positions independently", () => {
    const snap = {
      nodes: [node("a", 1)],
      viewport: { x: 0, y: 0, zoom: 1 },
    };
    const copy = cloneFlowLayoutHistorySnapshot(snap);
    copy.nodes[0]!.position.x = 99;
    expect(snap.nodes[0]!.position.x).toBe(1);
  });

  it("pushFlowLayoutHistory caps stack", () => {
    let past: ReturnType<typeof cloneFlowLayoutHistorySnapshot>[] = [];
    for (let i = 0; i < 55; i++) {
      past = pushFlowLayoutHistory(past, { nodes: [node(`n${i}`, i)], viewport: null });
    }
    expect(past.length).toBe(50);
  });
});
