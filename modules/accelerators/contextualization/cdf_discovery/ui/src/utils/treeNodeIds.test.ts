import { describe, expect, it } from "vitest";
import {
  DATA_SAVED_QUERIES,
  TRANSFORM_PIPELINES,
  TRANSFORM_PIPELINE_PREFIX,
  TRANSFORM_WORKFLOW_PREFIX,
  dedupeNodeIds,
  isTransformWorkflowsSubtreeNodeId,
} from "./treeNodeIds";

describe("dedupeNodeIds", () => {
  it("trims, dedupes, and drops empty entries", () => {
    expect(dedupeNodeIds([" dm ", "dm", "", "data:sq", "data:sq"])).toEqual(["dm", "data:sq"]);
  });

  it("does not rewrite legacy ids", () => {
    expect(dedupeNodeIds(["sq", DATA_SAVED_QUERIES])).toEqual(["sq", DATA_SAVED_QUERIES]);
  });
});

describe("isTransformWorkflowsSubtreeNodeId", () => {
  it("matches workflows folder, pipelines, and workflow yaml leaves", () => {
    expect(isTransformWorkflowsSubtreeNodeId(TRANSFORM_PIPELINES)).toBe(true);
    expect(isTransformWorkflowsSubtreeNodeId(`${TRANSFORM_PIPELINE_PREFIX}my_flow`)).toBe(true);
    expect(
      isTransformWorkflowsSubtreeNodeId(`${TRANSFORM_WORKFLOW_PREFIX}my_flow:Workflow`)
    ).toBe(true);
    expect(isTransformWorkflowsSubtreeNodeId("data:dm")).toBe(false);
  });
});
