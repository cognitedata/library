import { describe, expect, it } from "vitest";
import { applyPaletteStarredFlags, sortPaletteTreeNodes } from "./paletteStars";
import type { TreeNode } from "../types/dataTree";

const node = (id: string, label: string): TreeNode => ({
  id,
  label,
  kind: "folder",
  has_children: true,
});

describe("paletteStars", () => {
  it("sorts starred siblings first then label", () => {
    const sorted = sortPaletteTreeNodes(
      [node("m", "Middle"), node("z", "Zulu"), node("a", "Alpha")],
      ["z", "a"]
    );
    expect(sorted.map((n) => n.id)).toEqual(["z", "a", "m"]);
  });

  it("applies starred flag", () => {
    const out = applyPaletteStarredFlags([node("a", "A")], new Set(["a"]));
    expect(out[0].starred).toBe(true);
  });
});
