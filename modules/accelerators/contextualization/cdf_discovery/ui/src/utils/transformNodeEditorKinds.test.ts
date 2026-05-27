import { describe, expect, it } from "vitest";
import {
  isOrchestrationNodeKind,
  shouldOpenNodeEditorOnDoubleClick,
} from "./transformNodeEditorKinds";

describe("transformNodeEditorKinds", () => {
  it("identifies orchestration canvas kinds", () => {
    expect(isOrchestrationNodeKind("function_ref")).toBe(true);
    expect(isOrchestrationNodeKind("cdf_task")).toBe(true);
    expect(isOrchestrationNodeKind("query_view")).toBe(false);
  });

  it("opens orchestration editors in read-only workflow view", () => {
    expect(shouldOpenNodeEditorOnDoubleClick("subworkflow", true)).toBe(true);
    expect(shouldOpenNodeEditorOnDoubleClick("transform", true)).toBe(false);
  });

  it("opens modal editor kinds on the pipeline canvas", () => {
    expect(shouldOpenNodeEditorOnDoubleClick("join", false)).toBe(true);
    expect(shouldOpenNodeEditorOnDoubleClick(null, false)).toBe(false);
  });
});
