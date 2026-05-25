import { describe, expect, it } from "vitest";
import { sortTreeNodes } from "./discoveryStars";
import type { TreeNode } from "../types/discoveryNodes";

function folder(id: string, label: string): TreeNode {
  return { id, label, kind: "folder", has_children: true };
}

describe("sortTreeNodes connection root", () => {
  it("keeps Transform above Project when labels would sort otherwise", () => {
    const nodes = [
      folder("transform", "Transform"),
      { id: "connection:info", label: "Project: acme", kind: "connection", has_children: false },
      folder("data", "Data"),
      folder("gov", "Governance"),
      folder("fusion", "Fusion"),
    ];
    const sorted = sortTreeNodes(nodes, []);
    expect(sorted.map((n) => n.id)).toEqual([
      "data",
      "fusion",
      "gov",
      "transform",
      "connection:info",
    ]);
  });
});
