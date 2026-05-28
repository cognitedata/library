import { describe, expect, it } from "vitest";
import { sortTreeNodes } from "./discoveryStars";
import type { TreeNode } from "../types/discoveryNodes";

function folder(id: string, label: string): TreeNode {
  return { id, label, kind: "folder", has_children: true };
}

describe("sortTreeNodes connection root", () => {
  it("orders top-level domains by canonical id, not label", () => {
    const nodes = [
      folder("transform", "Transform"),
      folder("monitor", "Monitor"),
      folder("data", "Data"),
      folder("gov", "Governance"),
      folder("fusion", "Fusion"),
      folder("extract", "Extract"),
    ];
    const sorted = sortTreeNodes(nodes, []);
    expect(sorted.map((n) => n.id)).toEqual([
      "data",
      "fusion",
      "gov",
      "extract",
      "transform",
      "monitor",
    ]);
  });
});
