import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";

describe("assignFlowNodeSubflowParent", () => {
  it("clears parentId when assigning root (null parent)", () => {
    const child: Node = {
      id: "c",
      type: "keaExtraction",
      parentId: "legacy",
      position: { x: 10, y: 20 },
      data: { label: "E", handler_id: "h" },
    };
    const nodes: Node[] = [child];
    const out = assignFlowNodeSubflowParent(nodes, "c", null);
    expect(out.find((n) => n.id === "c")?.parentId).toBeUndefined();
  });

  it("ignores start nodes", () => {
    const s: Node = { id: "s", type: "keaStart", position: { x: 0, y: 0 }, data: { label: "S" } };
    const out = assignFlowNodeSubflowParent([s], "s", null);
    expect(out.find((n) => n.id === "s")?.parentId).toBeUndefined();
  });

  it("does not change hub nodes", () => {
    const hub: Node = {
      id: "sf_hub_in",
      type: "keaSubflowGraphIn",
      parentId: "sf",
      position: { x: 10, y: 40 },
      data: {},
    };
    const nodes: Node[] = [hub];
    expect(assignFlowNodeSubflowParent(nodes, "sf_hub_in", null)).toEqual(nodes);
  });

  it("ignores non-null parent ids (no arbitrary parent assignment)", () => {
    const a: Node = { id: "a", type: "keaExtraction", position: { x: 0, y: 0 }, data: { label: "A", handler_id: "h" } };
    const sf: Node = {
      id: "sf",
      type: "keaSubgraph",
      position: { x: 0, y: 0 },
      data: { label: "G" },
      style: { width: 200, height: 160 },
    };
    const nodes: Node[] = [sf, a];
    const out = assignFlowNodeSubflowParent(nodes, "a", "sf");
    expect(out).toEqual(nodes);
  });
});
