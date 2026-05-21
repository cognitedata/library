import { describe, expect, it } from "vitest";
import {
  cascadeDisableIds,
  cascadeEnableIds,
  patchWorkflowCanvasNodeEnabled,
} from "./flowNodeEnabled";
import {
  isWorkflowCanvasNodeCascadeDisabled,
  isWorkflowCanvasNodeEnabled,
  type WorkflowCanvasDocument,
} from "../../types/workflowCanvas";

const VQ = {
  id: "vq",
  kind: "query_view" as const,
  position: { x: 0, y: 0 },
  data: {
    config: {
      description: "q0",
      view_space: "cdf_cdm",
      view_external_id: "CogniteFile",
      view_version: "v1",
    },
  },
};
const VQ_B = {
  id: "vqB",
  kind: "query_view" as const,
  position: { x: 0, y: 0 },
  data: {
    config: {
      description: "q1",
      view_space: "cdf_cdm",
      view_external_id: "CogniteFile",
      view_version: "v1",
    },
  },
};
const TR = {
  id: "tr",
  kind: "transform" as const,
  position: { x: 0, y: 0 },
  data: { config: { description: "t0" } },
};
const VA = {
  id: "va",
  kind: "validation" as const,
  position: { x: 0, y: 0 },
  data: { config: { description: "v0" } },
};

function canvas(
  nodes: WorkflowCanvasDocument["nodes"],
  edges: WorkflowCanvasDocument["edges"]
): WorkflowCanvasDocument {
  return { schemaVersion: 1, nodes, edges };
}

describe("cascade disable/enable", () => {
  it("cascade-disables a linear chain", () => {
    const doc = canvas(
      [VQ, TR, VA],
      [
        { id: "e1", source: "vq", target: "tr" },
        { id: "e2", source: "tr", target: "va" },
      ]
    );
    expect(cascadeDisableIds(doc, new Set(["vq"]))).toEqual(new Set(["vq", "tr", "va"]));
  });

  it("keeps join enabled when one parallel query stays on", () => {
    const join = {
      id: "jn",
      kind: "join" as const,
      position: { x: 0, y: 0 },
      data: {
        config: {
          description: "j",
          join_on: { operator: "EQUALS", left_property: "a", right_property: "b" },
        },
      },
    };
    const doc = canvas(
      [VQ, VQ_B, join],
      [
        { id: "e1", source: "vq", target: "jn", target_handle: "in__left" },
        { id: "e2", source: "vqB", target: "jn", target_handle: "in__right" },
      ]
    );
    const one = cascadeDisableIds(doc, new Set(["vq"]));
    expect(one.has("jn")).toBe(false);
    expect(one.has("vqB")).toBe(false);
    expect(cascadeDisableIds(doc, new Set(["vq", "vqB"]))).toEqual(new Set(["vq", "vqB", "jn"]));
  });

  it("cascade-enables only cascade-marked nodes when upstream returns", () => {
    const doc = canvas(
      [VQ, TR, VA],
      [
        { id: "e1", source: "vq", target: "tr" },
        { id: "e2", source: "tr", target: "va" },
      ]
    );
    const disabled = patchWorkflowCanvasNodeEnabled(doc, "vq", false);
    const byId = Object.fromEntries(disabled.document.nodes.map((n) => [n.id, n]));
    expect(isWorkflowCanvasNodeEnabled(byId.tr!)).toBe(false);
    expect(isWorkflowCanvasNodeCascadeDisabled(byId.tr!)).toBe(true);

    const enabled = patchWorkflowCanvasNodeEnabled(disabled.document, "vq", true);
    const byId2 = Object.fromEntries(enabled.document.nodes.map((n) => [n.id, n]));
    expect(isWorkflowCanvasNodeEnabled(byId2.vq!)).toBe(true);
    expect(isWorkflowCanvasNodeEnabled(byId2.tr!)).toBe(true);
    expect(isWorkflowCanvasNodeEnabled(byId2.va!)).toBe(true);
  });

  it("does not re-enable manually disabled nodes", () => {
    const doc = canvas([VQ, TR], [{ id: "e1", source: "vq", target: "tr" }]);
    const step1 = patchWorkflowCanvasNodeEnabled(doc, "vq", false);
    const step2 = patchWorkflowCanvasNodeEnabled(step1.document, "tr", false);
    const byId = Object.fromEntries(step2.document.nodes.map((n) => [n.id, n]));
    expect(isWorkflowCanvasNodeCascadeDisabled(byId.tr!)).toBe(false);

    const step3 = patchWorkflowCanvasNodeEnabled(step2.document, "vq", true);
    const byId3 = Object.fromEntries(step3.document.nodes.map((n) => [n.id, n]));
    expect(isWorkflowCanvasNodeEnabled(byId3.vq!)).toBe(true);
    expect(isWorkflowCanvasNodeEnabled(byId3.tr!)).toBe(false);
  });

  it("cascade_enable_ids requires pending_enable when root is still off", () => {
    const doc = canvas([VQ, TR], [{ id: "e1", source: "vq", target: "tr" }]);
    const disabled = patchWorkflowCanvasNodeEnabled(doc, "vq", false);
    expect(cascadeEnableIds(disabled.document, new Set(["vq"]))).toEqual(new Set());
    const enabled = patchWorkflowCanvasNodeEnabled(disabled.document, "vq", true);
    expect(enabled.cascadeAffectedIds).toEqual(["tr"]);
  });

  it("cascade disable propagates through subgraph frame and inner_canvas", () => {
    const sg = {
      id: "sg",
      kind: "subgraph" as const,
      position: { x: 0, y: 0 },
      data: {
        subflow_hub_input_id: "sg_in",
        subflow_hub_output_id: "sg_out",
        inner_canvas: canvas(
          [
            { id: "sg_in", kind: "subflow_graph_in" as const, position: { x: 0, y: 0 }, data: {} },
            {
              id: "rq",
              kind: "query_raw" as const,
              position: { x: 0, y: 0 },
              data: { config: { description: "r0", raw_db: "db" } },
            },
            { id: "sg_out", kind: "subflow_graph_out" as const, position: { x: 0, y: 0 }, data: {} },
          ],
          [
            {
              id: "e_in",
              source: "sg_in",
              target: "rq",
              source_handle: "out__in",
              target_handle: "in",
            },
            {
              id: "e_out",
              source: "rq",
              target: "sg_out",
              source_handle: "out",
              target_handle: "in__out",
            },
          ]
        ),
      },
    };
    const tr2 = {
      id: "tr2",
      kind: "transform" as const,
      position: { x: 0, y: 0 },
      data: { config: { description: "t1" } },
    };
    const doc = canvas(
      [VQ, sg, tr2],
      [
        { id: "e0", source: "vq", target: "sg", target_handle: "in__in" },
        { id: "e1", source: "sg", target: "tr2", source_handle: "out__out" },
      ]
    );
    const disabled = patchWorkflowCanvasNodeEnabled(doc, "vq", false);
    const byId = Object.fromEntries(disabled.document.nodes.map((n) => [n.id, n]));
    expect(isWorkflowCanvasNodeEnabled(byId.sg!)).toBe(false);
    expect(isWorkflowCanvasNodeCascadeDisabled(byId.sg!)).toBe(true);
    expect(isWorkflowCanvasNodeEnabled(byId.tr2!)).toBe(false);
    const inner = byId.sg!.data.inner_canvas!;
    const rq = inner.nodes.find((n) => n.id === "rq");
    expect(rq && isWorkflowCanvasNodeEnabled(rq)).toBe(false);
    expect(rq && isWorkflowCanvasNodeCascadeDisabled(rq)).toBe(true);
  });

  it("keeps downstream after enabled subgraph when upstream is valid", () => {
    const sg = {
      id: "sg",
      kind: "subgraph" as const,
      position: { x: 0, y: 0 },
      data: {
        subflow_hub_input_id: "sg_in",
        subflow_hub_output_id: "sg_out",
        inner_canvas: canvas(
          [
            { id: "sg_in", kind: "subflow_graph_in" as const, position: { x: 0, y: 0 }, data: {} },
            {
              id: "rq",
              kind: "query_raw" as const,
              position: { x: 0, y: 0 },
              data: { config: { description: "r0", raw_db: "db" } },
            },
            { id: "sg_out", kind: "subflow_graph_out" as const, position: { x: 0, y: 0 }, data: {} },
          ],
          [
            {
              id: "e_in",
              source: "sg_in",
              target: "rq",
              source_handle: "out__in",
              target_handle: "in",
            },
            {
              id: "e_out",
              source: "rq",
              target: "sg_out",
              source_handle: "out",
              target_handle: "in__out",
            },
          ]
        ),
      },
    };
    const tr2 = {
      id: "tr2",
      kind: "transform" as const,
      position: { x: 0, y: 0 },
      data: { config: { description: "t1" } },
    };
    const doc = canvas(
      [VQ, sg, tr2],
      [
        { id: "e0", source: "vq", target: "sg", target_handle: "in__in" },
        { id: "e1", source: "sg", target: "tr2", source_handle: "out__out" },
      ]
    );
    const disabled = cascadeDisableIds(doc, new Set(["vq"]));
    expect(disabled.has("tr2")).toBe(true);
    expect(cascadeDisableIds(doc, new Set())).toEqual(new Set());
  });
});
