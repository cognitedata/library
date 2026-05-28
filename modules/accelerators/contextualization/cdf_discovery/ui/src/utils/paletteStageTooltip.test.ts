import { describe, expect, it } from "vitest";
import { paletteStageDocKey, paletteStageTooltip } from "./paletteStageTooltip";

const t = (key: string) =>
  ({
    "transform.paletteDoc.query_view": "View query verbose",
    "transform.palette.query_view": "View query",
  })[key] ?? key;

describe("paletteStageTooltip", () => {
  it("returns paletteDoc text when present", () => {
    expect(paletteStageTooltip("query_view", t)).toBe("View query verbose");
  });

  it("falls back to palette label when doc text is missing", () => {
    expect(paletteStageTooltip("filter", (k) => k)).toBe("transform.palette.filter");
  });

  it("maps known stages to paletteDoc keys", () => {
    expect(paletteStageDocKey("save_raw")).toBe("transform.paletteDoc.save_raw");
    expect(paletteStageDocKey("score")).toBe("transform.paletteDoc.score");
  });
});
