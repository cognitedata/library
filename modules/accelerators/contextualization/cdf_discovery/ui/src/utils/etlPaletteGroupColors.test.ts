import { describe, expect, it } from "vitest";
import {
  defaultNodeColorForStage,
  paletteGroupIdForStage,
  paletteGroupIdForTransformHandler,
} from "./etlPaletteGroupColors";

describe("etlPaletteGroupColors", () => {
  it("maps extract and load stages", () => {
    expect(paletteGroupIdForStage("query_view")).toBe("extract");
    expect(paletteGroupIdForStage("save_raw")).toBe("load");
  });

  it("maps transform handlers to handler categories", () => {
    expect(paletteGroupIdForTransformHandler("trim_whitespace")).toBe("transform_string");
    expect(paletteGroupIdForTransformHandler("split_join")).toBe("transform_structure");
    expect(paletteGroupIdForTransformHandler("hash_stable")).toBe("transform_derive");
  });

  it("maps score and build_index", () => {
    expect(paletteGroupIdForStage("score")).toBe("transform_derive");
    expect(paletteGroupIdForStage("build_index")).toBe("contextualization");
  });

  it("returns hex for new nodes", () => {
    expect(defaultNodeColorForStage("transform", "trim_whitespace")).toBe("#0891b2");
    expect(defaultNodeColorForStage("file_annotation")).toBe("#d97706");
  });
});
