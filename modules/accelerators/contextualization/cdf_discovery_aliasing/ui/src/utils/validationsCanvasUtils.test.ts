import { describe, expect, it } from "vitest";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  listValidationNodeRefs,
  listValidationNodes,
  patchValidationNode,
  validationNodeContainerLabel,
  validationNodeLocationKey,
} from "./validationsCanvasUtils";

describe("validationsCanvasUtils", () => {
  it("lists validation nodes inside subgraph inner_canvas", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        {
          id: "sg1",
          kind: "subgraph",
          position: { x: 0, y: 0 },
          data: {
            label: "Inverted Index Validation",
            inner_canvas: {
              schemaVersion: 1,
              nodes: [
                {
                  id: "v1",
                  kind: "validation",
                  position: { x: 0, y: 0 },
                  data: { label: "Blacklist", config: { description: "Blacklist rules" } },
                },
                {
                  id: "v2",
                  kind: "validation",
                  position: { x: 10, y: 0 },
                  data: { config: { description: "Shape rules" } },
                },
              ],
              edges: [],
            },
          },
        },
      ],
      edges: [],
    };
    const refs = listValidationNodeRefs(canvas);
    expect(refs).toHaveLength(2);
    expect(refs[0]!.node.id).toBe("v1");
    expect(refs[0]!.subgraphPath).toEqual(["sg1"]);
    expect(listValidationNodes(canvas)).toHaveLength(2);
    expect(validationNodeContainerLabel(canvas, ["sg1"])).toBe("Inverted Index Validation");
  });

  it("patches validation config inside a subgraph", () => {
    const canvas: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        {
          id: "sg1",
          kind: "subgraph",
          position: { x: 0, y: 0 },
          data: {
            inner_canvas: {
              schemaVersion: 1,
              nodes: [
                {
                  id: "v1",
                  kind: "validation",
                  position: { x: 0, y: 0 },
                  data: { config: { description: "before" } },
                },
              ],
              edges: [],
            },
          },
        },
      ],
      edges: [],
    };
    const next = patchValidationNode(canvas, "v1", { description: "after" }, ["sg1"]);
    const cfg = listValidationNodeRefs(next)[0]!.node.data?.config as { description?: string };
    expect(cfg.description).toBe("after");
  });

  it("uses stable location keys for nested nodes", () => {
    const ref = {
      node: { id: "v1", kind: "validation" as const, position: { x: 0, y: 0 }, data: {} },
      subgraphPath: ["sg1"],
    };
    expect(validationNodeLocationKey(ref)).toBe("sg1:v1");
  });
});
