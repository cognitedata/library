import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { resolveSubflowParentsAfterGroupDrag } from "./subflowDropAssociation";

describe("resolveSubflowParentsAfterGroupDrag", () => {
  it("parents two selected nodes whose centers lie inside the same subflow", () => {
    const sf: Node = {
      id: "sf1",
      type: "keaSubflow",
      position: { x: 0, y: 0 },
      data: { label: "S" },
      style: { width: 500, height: 400 },
      selected: false,
    };
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 150, y: 150 },
      data: { label: "A", handler_id: "h" },
      selected: true,
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      position: { x: 250, y: 200 },
      data: { label: "B", handler_id: "h2" },
      selected: true,
    };
    const nodes: Node[] = [sf, a, b];
    const next = resolveSubflowParentsAfterGroupDrag(nodes, "a");
    expect(next).not.toBeNull();
    const ua = next!.find((n) => n.id === "a");
    const ub = next!.find((n) => n.id === "b");
    expect(ua?.parentId).toBe("sf1");
    expect(ub?.parentId).toBe("sf1");
    const sfIdx = next!.findIndex((n) => n.id === "sf1");
    const aIdx = next!.findIndex((n) => n.id === "a");
    expect(sfIdx).toBeLessThan(aIdx);
  });
});
