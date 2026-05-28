import { describe, expect, it } from "vitest";
import { canvasValidationNodeIds } from "./canvasValidationNodeIds";

describe("canvasValidationNodeIds", () => {
  it("parses node ids from compile errors", () => {
    expect(
      canvasValidationNodeIds([
        "Compile failed: transform node 't1': output_field is required",
        "transform node 'join_1' step[0]: at least one fields[].field_name is required",
      ])
    ).toEqual(["t1", "join_1"]);
  });

  it("returns empty for ok validation", () => {
    expect(canvasValidationNodeIds([])).toEqual([]);
    expect(canvasValidationNodeIds(undefined)).toEqual([]);
  });
});
