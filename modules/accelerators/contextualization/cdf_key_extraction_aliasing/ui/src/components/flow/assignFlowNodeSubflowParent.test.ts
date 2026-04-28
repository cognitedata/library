import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";

describe("assignFlowNodeSubflowParent", () => {
  it("assigns a valid child under a subflow with relative position", () => {
    const sf: Node = {
      id: "sf",
      type: "keaSubflow",
      position: { x: 50, y: 60 },
      data: { label: "G" },
      style: { width: 300, height: 200 },
    };
    const child: Node = {
      id: "c",
      type: "keaExtraction",
      position: { x: 200, y: 100 },
      data: { label: "E", handler_id: "h" },
    };
    const nodes: Node[] = [sf, child];
    const out = assignFlowNodeSubflowParent(nodes, "c", "sf");
    const u = out.find((n) => n.id === "c");
    expect(u?.parentId).toBe("sf");
    expect(u?.position.x).toBe(150);
    expect(u?.position.y).toBe(40);
  });

  it("ignores start nodes", () => {
    const s: Node = { id: "s", type: "keaStart", position: { x: 0, y: 0 }, data: { label: "S" } };
    const sf: Node = {
      id: "sf",
      type: "keaSubflow",
      position: { x: 10, y: 10 },
      data: {},
      style: { width: 100, height: 100 },
    };
    const out = assignFlowNodeSubflowParent([s, sf], "s", "sf");
    expect(out.find((n) => n.id === "s")?.parentId).toBeUndefined();
  });

  it("rejects reparenting subgraph hub nodes", () => {
    const sf: Node = {
      id: "sf",
      type: "keaSubflow",
      position: { x: 0, y: 0 },
      data: {},
      style: { width: 200, height: 160 },
    };
    const hub: Node = {
      id: "sf_hub_in",
      type: "keaSubflowGraphIn",
      parentId: "sf",
      position: { x: 10, y: 40 },
      data: {},
    };
    const nodes: Node[] = [sf, hub];
    expect(assignFlowNodeSubflowParent(nodes, "sf_hub_in", null)).toEqual(nodes);
    expect(assignFlowNodeSubflowParent(nodes, "sf_hub_in", "other")).toEqual(nodes);
  });

  it("rejects a non-subflow parent id", () => {
    const a: Node = { id: "a", type: "keaExtraction", position: { x: 0, y: 0 }, data: { label: "A", handler_id: "h" } };
    const b: Node = { id: "b", type: "keaAliasing", position: { x: 100, y: 0 }, data: { label: "B", handler_id: "h2" } };
    const nodes: Node[] = [a, b];
    const out = assignFlowNodeSubflowParent(nodes, "a", "b");
    expect(out).toEqual(nodes);
  });
});
