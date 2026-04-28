import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import { layoutFlowNodes } from "./autoLayoutFlow";

describe("layoutFlowNodes / subflow interior", () => {
  it("lays out children inside a subflow and resizes the frame", () => {
    const sf: Node = {
      id: "sf",
      type: "keaSubflow",
      position: { x: 0, y: 0 },
      data: { label: "Group" },
      style: { width: 400, height: 300 },
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      parentId: "sf",
      position: { x: 5, y: 5 },
      data: { label: "A", handler_id: "h1" },
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      parentId: "sf",
      position: { x: 5, y: 5 },
      data: { label: "B", handler_id: "h2" },
    };
    const edges: Edge[] = [
      { id: "e1", source: "a", target: "b", data: { kind: "data" } },
    ];
    const out = layoutFlowNodes([sf, a, b], edges, "lr");
    const sfn = out.find((n) => n.id === "sf")!;
    const w = (sfn.style as { width?: number }).width;
    const h = (sfn.style as { height?: number }).height;
    expect(typeof w).toBe("number");
    expect(typeof h).toBe("number");
    expect(w!).toBeGreaterThanOrEqual(200);
    expect(h!).toBeGreaterThanOrEqual(140);
    const na = out.find((n) => n.id === "a")!;
    const nb = out.find((n) => n.id === "b")!;
    expect(na.parentId).toBe("sf");
    expect(nb.parentId).toBe("sf");
    expect(na.position.x).toBeLessThan(nb.position.x);
  });
});
