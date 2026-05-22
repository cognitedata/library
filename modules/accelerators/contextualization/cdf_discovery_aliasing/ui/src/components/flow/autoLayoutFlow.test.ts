import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { layoutFlowNodes } from "./autoLayoutFlow";

describe("layoutFlowNodes", () => {
  it("orders same-layer discoveryTransform by pipeline_rank, not node id", () => {
    const st: Node = { id: "st", type: "discoveryStart", position: { x: 0, y: 0 }, data: {} };
    const q: Node = { id: "q", type: "discoveryViewQuery", position: { x: 0, y: 0 }, data: { label: "q" } };
    const trC: Node = {
      id: "tr_c",
      type: "discoveryTransform",
      position: { x: 0, y: 0 },
      data: { label: "c", pipeline_rank: 1 },
    };
    const trS: Node = {
      id: "tr_s",
      type: "discoveryTransform",
      position: { x: 0, y: 0 },
      data: { label: "s", pipeline_rank: 0 },
    };
    const trL: Node = {
      id: "tr_l",
      type: "discoveryTransform",
      position: { x: 0, y: 0 },
      data: { label: "l", pipeline_rank: 2 },
    };
    const en: Node = { id: "en", type: "discoveryEnd", position: { x: 0, y: 0 }, data: {} };
    const edges: Edge[] = [
      { id: "e0", source: "st", target: "q", data: { kind: "data" } },
      { id: "e1", source: "q", target: "tr_c", data: { kind: "data" } },
      { id: "e2", source: "q", target: "tr_s", data: { kind: "data" } },
      { id: "e3", source: "q", target: "tr_l", data: { kind: "data" } },
      { id: "e4", source: "tr_c", target: "en", data: { kind: "data" } },
      { id: "e5", source: "tr_s", target: "en", data: { kind: "data" } },
      { id: "e6", source: "tr_l", target: "en", data: { kind: "data" } },
    ];
    const out = layoutFlowNodes([st, q, trC, trS, trL, en], edges, "lr");
    const pS = out.find((n) => n.id === "tr_s")!.position.y;
    const pC = out.find((n) => n.id === "tr_c")!.position.y;
    const pL = out.find((n) => n.id === "tr_l")!.position.y;
    expect(pS).toBeLessThan(pC);
    expect(pC).toBeLessThan(pL);
  });
});
