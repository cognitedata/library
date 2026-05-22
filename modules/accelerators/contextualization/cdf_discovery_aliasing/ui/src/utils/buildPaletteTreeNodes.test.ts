import { describe, expect, it } from "vitest";
import { buildPaletteTreeChildrenByParent } from "./buildPaletteTreeNodes";

const t = (key: string) => key;

describe("buildPaletteTreeNodes", () => {
  it("includes transform leaves with display name keys resolved via t", () => {
    const map = buildPaletteTreeChildrenByParent(t, {});
    const transforms = map.get("palette:transform") ?? [];
    const trim = transforms.find((n) => n.id === "palette:leaf:transform:trim_whitespace");
    expect(trim?.label).toBe("transforms.handlerName.trim_whitespace");
    expect(trim?.meta?.palette_payload).toEqual({
      kind: "discovery",
      stage: "transform",
      transformHandlerId: "trim_whitespace",
    });
  });

  it("exposes pipeline and CDF data roots", () => {
    const map = buildPaletteTreeChildrenByParent(t, {});
    const root = map.get("palette_root") ?? [];
    const dataNode = root.find((n) => n.id === "data");
    expect(dataNode?.has_children).toBe(true);
    // data branch children loaded via API — not in static map at data key until fetch
    expect(map.has("palette:query")).toBe(true);
  });
});
