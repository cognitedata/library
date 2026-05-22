import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { resolveSubflowParentsAfterGroupDrag } from "./subflowDropAssociation";

describe("resolveSubflowParentsAfterGroupDrag", () => {
  it("returns null (subflow group drag removed)", () => {
    const sf: Node = {
      id: "sf1",
      type: "discoverySubgraph",
      position: { x: 0, y: 0 },
      data: { label: "S" },
      style: { width: 500, height: 400 },
      selected: false,
    };
    const a: Node = {
      id: "a",
      type: "discoveryTransform",
      position: { x: 150, y: 150 },
      data: { label: "A", handler_id: "h" },
      selected: true,
    };
    const nodes: Node[] = [sf, a];
    expect(resolveSubflowParentsAfterGroupDrag(nodes, "a")).toBeNull();
  });
});
