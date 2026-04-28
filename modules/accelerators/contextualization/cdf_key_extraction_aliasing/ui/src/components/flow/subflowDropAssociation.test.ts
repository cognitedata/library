import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { resolveSubflowParentAfterDrag } from "./subflowDropAssociation";

describe("resolveSubflowParentAfterDrag", () => {
  it("parents a root node whose center lies inside a subflow and sets position relative to that subflow", () => {
    const sf: Node = {
      id: "sf1",
      type: "keaSubflow",
      position: { x: 100, y: 100 },
      data: { label: "S" },
      style: { width: 380, height: 260 },
    };
    const dropped: Node = {
      id: "n1",
      type: "keaExtraction",
      position: { x: 200, y: 180 },
      data: { label: "E", handler_id: "h" },
    };
    const nodes: Node[] = [sf, dropped];
    const next = resolveSubflowParentAfterDrag(nodes, dropped);
    expect(next).not.toBeNull();
    const u = next!.find((n) => n.id === "n1");
    expect(u?.parentId).toBe("sf1");
    expect(u?.position.x).toBe(100);
    expect(u?.position.y).toBe(80);
  });

  it("returns null when the node center is outside all subflows", () => {
    const sf: Node = {
      id: "sf1",
      type: "keaSubflow",
      position: { x: 400, y: 400 },
      data: { label: "S" },
      style: { width: 200, height: 200 },
    };
    const dropped: Node = {
      id: "n1",
      type: "keaExtraction",
      position: { x: 20, y: 20 },
      data: { label: "E", handler_id: "h" },
    };
    expect(resolveSubflowParentAfterDrag([sf, dropped], dropped)).toBeNull();
  });
});
