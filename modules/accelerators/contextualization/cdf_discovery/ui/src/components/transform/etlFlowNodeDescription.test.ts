import { describe, expect, it } from "vitest";
import { etlFlowNodeCanvasDescription } from "./etlFlowNodeDescription";

describe("etlFlowNodeCanvasDescription", () => {
  it("prefers config summary over description and notes", () => {
    const text = etlFlowNodeCanvasDescription("query_view", {
      config: {
        description: "My query",
        view_space: "sp",
        view_external_id: "Asset",
      },
      notes: "Inspector notes",
    });
    expect(text).toBe("sp/Asset");
  });

  it("falls back to config.description when summary is empty", () => {
    expect(
      etlFlowNodeCanvasDescription("transform", {
        config: { description: "Normalize names" },
      })
    ).toBe("Normalize names");
  });

  it("falls back to notes when config has no summary or description", () => {
    expect(
      etlFlowNodeCanvasDescription("end", {
        notes: "Terminal step",
      })
    ).toBe("Terminal step");
  });
});
