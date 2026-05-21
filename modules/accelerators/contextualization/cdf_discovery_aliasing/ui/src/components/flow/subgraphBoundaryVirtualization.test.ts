import { describe, expect, it } from "vitest";
import {
  SUBFLOW_PORT_HANDLE_IN_PREFIX,
  WORKFLOW_CANVAS_SCHEMA_VERSION,
  subflowSourceHandleForPort,
} from "../../types/workflowCanvas";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { flattenSubgraphsForScopeSync } from "./subgraphBoundaryVirtualization";

describe("flattenSubgraphsForScopeSync", () => {
  it("hoists inner transform so outer data edges reach prefixed inner node ids", () => {
    const hubInId = "hub_in_1";
    const hubOutId = "hub_out_1";
    const innerTrId = "inner_tr";
    const sgId = "subgraph_box";

    const inner: WorkflowCanvasDocument = {
      schemaVersion: WORKFLOW_CANVAS_SCHEMA_VERSION,
      nodes: [
        { id: hubInId, kind: "subflow_graph_in", position: { x: 0, y: 0 }, data: { label: "In" } },
        { id: hubOutId, kind: "subflow_graph_out", position: { x: 400, y: 0 }, data: { label: "Out" } },
        {
          id: innerTrId,
          kind: "transform",
          position: { x: 120, y: 0 },
          data: { label: "inner_tr", config: { description: "t" } },
        },
      ],
      edges: [
        {
          id: "e_hub_to_tr",
          source: hubInId,
          target: innerTrId,
          kind: "data",
          source_handle: subflowSourceHandleForPort("in"),
          target_handle: "in",
        },
      ],
    };

    const doc: WorkflowCanvasDocument = {
      schemaVersion: WORKFLOW_CANVAS_SCHEMA_VERSION,
      nodes: [
        { id: "q_o", kind: "query_view", position: { x: 0, y: 0 }, data: { config: { description: "q" } } },
        {
          id: sgId,
          kind: "subgraph",
          position: { x: 100, y: 0 },
          data: {
            label: "G",
            subflow_hub_input_id: hubInId,
            subflow_hub_output_id: hubOutId,
            inner_canvas: inner,
          },
        },
      ],
      edges: [
        {
          id: "e_q_to_sg",
          source: "q_o",
          target: sgId,
          kind: "data",
          source_handle: "out",
          target_handle: `${SUBFLOW_PORT_HANDLE_IN_PREFIX}in`,
        },
      ],
    };

    const flat = flattenSubgraphsForScopeSync(doc);
    expect(flat.nodes.some((n) => n.kind === "subgraph")).toBe(false);
    const liftedTr = flat.nodes.find((n) => n.kind === "transform");
    expect(liftedTr).toBeDefined();
    expect(liftedTr!.id.startsWith(`__sg_`)).toBe(true);
    expect(
      flat.edges.some((e) => e.kind === "data" && e.source === "q_o" && e.target === liftedTr!.id)
    ).toBe(true);
  });
});
