import { describe, expect, it } from "vitest";
import { collectStartCanvasNodeIdsOnAnyPathToTarget } from "./flowRunProgressEdges";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";

describe("collectStartCanvasNodeIdsOnAnyPathToTarget", () => {
  it("finds start upstream of a discovery node", () => {
    const doc: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        { id: "st", kind: "start", position: { x: 0, y: 0 }, data: { label: "Start" } },
        {
          id: "vq",
          kind: "query_view",
          position: { x: 100, y: 0 },
          data: {
            config: {
              description: "q",
              view_space: "cdf_cdm",
              view_external_id: "CogniteFile",
              view_version: "v1",
            },
          },
        },
      ],
      edges: [{ id: "e1", source: "st", target: "vq", kind: "data" }],
    };
    expect(collectStartCanvasNodeIdsOnAnyPathToTarget(doc, "vq")).toEqual(["st"]);
  });

  it("returns empty when target missing or no path to start", () => {
    const doc: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [{ id: "vq", kind: "query_view", position: { x: 0, y: 0 }, data: {} }],
      edges: [],
    };
    expect(collectStartCanvasNodeIdsOnAnyPathToTarget(doc, "vq")).toEqual([]);
    expect(collectStartCanvasNodeIdsOnAnyPathToTarget(doc, "missing")).toEqual([]);
  });
});
