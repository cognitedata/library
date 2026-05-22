import type { Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { materializeDataTreeEntityDrop } from "./materializeDataTreeEntityDrop";
import type { TreeNode } from "../../types/dataTree";

describe("materializeDataTreeEntityDrop", () => {
  const t = (key: string) => key;

  const dmView: TreeNode = {
    id: "v1",
    label: "MyView (v1)",
    kind: "dm_view",
    has_children: false,
    open_target: {
      type: "dm_instances",
      view_space: "sp",
      view_external_id: "ViewExt",
      view_version: "v1",
    },
  };

  it("creates query and save nodes with edges to start and end", () => {
    const end: Node = { id: "end1", type: "discoveryEnd", position: { x: 0, y: 0 }, data: {} };
    const start: Node = { id: "st1", type: "discoveryStart", position: { x: 0, y: 0 }, data: {} };
    const batch = materializeDataTreeEntityDrop({
      payload: { kind: "data_tree_entity", node: dmView },
      position: { x: 10, y: 10 },
      nodes: [start, end],
      t,
    });
    expect(batch).not.toBeNull();
    expect(batch!.nodes).toHaveLength(2);
    expect(batch!.nodes[0]!.type).toBe("discoveryViewQuery");
    expect(batch!.nodes[1]!.type).toBe("discoveryViewSave");
    expect(batch!.extraEdges.some((e) => e.source === "st1" && e.target === batch!.nodes[0]!.id)).toBe(
      true
    );
    expect(batch!.extraEdges.some((e) => e.source === batch!.nodes[0]!.id && e.target === batch!.nodes[1]!.id)).toBe(
      true
    );
    expect(batch!.extraEdges.some((e) => e.source === batch!.nodes[1]!.id && e.target === "end1")).toBe(true);
  });
});
