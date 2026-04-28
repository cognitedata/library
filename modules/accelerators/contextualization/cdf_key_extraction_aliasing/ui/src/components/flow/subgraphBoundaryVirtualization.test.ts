import { describe, expect, it } from "vitest";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { subflowSourceHandleForPort, subflowTargetHandleForPort } from "../../types/workflowCanvas";
import { expandCanvasForScopeSync } from "./subgraphBoundaryVirtualization";

describe("expandCanvasForScopeSync", () => {
  it("bridges source_view → subflow(in) and hub → extraction into source_view → extraction", () => {
    const sf = "sf1";
    const hubIn = `${sf}_hub_in`;
    const hubOut = `${sf}_hub_out`;
    const ext = "ext1";
    const sv = "sv1";
    const doc: WorkflowCanvasDocument = {
      schemaVersion: 1,
      nodes: [
        {
          id: sf,
          kind: "subflow",
          position: { x: 0, y: 0 },
          data: {
            subflow_ports: { inputs: [{ id: "in" }], outputs: [{ id: "out" }] },
            subflow_hub_input_id: hubIn,
            subflow_hub_output_id: hubOut,
          },
        },
        { id: hubIn, kind: "subflow_graph_in", parent_id: sf, position: { x: 0, y: 0 }, data: {} },
        { id: hubOut, kind: "subflow_graph_out", parent_id: sf, position: { x: 0, y: 0 }, data: {} },
        {
          id: ext,
          kind: "extraction",
          parent_id: sf,
          position: { x: 0, y: 0 },
          data: { ref: { extraction_rule_name: "r1" } },
        },
        { id: sv, kind: "source_view", position: { x: 0, y: 0 }, data: {} },
        { id: "end1", kind: "end", position: { x: 0, y: 0 }, data: {} },
      ],
      edges: [
        {
          id: "e_in",
          source: sv,
          target: sf,
          target_handle: subflowTargetHandleForPort("in"),
          kind: "data",
        },
        {
          id: "e_hub",
          source: hubIn,
          source_handle: subflowSourceHandleForPort("in"),
          target: ext,
          target_handle: "in",
          kind: "data",
        },
        {
          id: "e_out",
          source: ext,
          source_handle: "out",
          target: hubOut,
          target_handle: subflowTargetHandleForPort("out"),
          kind: "data",
        },
        {
          id: "e_sf_end",
          source: sf,
          source_handle: subflowSourceHandleForPort("out"),
          target: "end1",
          target_handle: "in",
          kind: "data",
        },
      ],
    };
    const out = expandCanvasForScopeSync(doc);
    expect(out.edges.some((e) => e.source === sv && e.target === ext)).toBe(true);
    expect(out.edges.some((e) => e.source === ext && e.target === "end1")).toBe(true);
    expect(out.edges.find((e) => e.id === "e_in")).toBeUndefined();
    expect(out.edges.find((e) => e.id === "e_hub")).toBeUndefined();
  });
});
