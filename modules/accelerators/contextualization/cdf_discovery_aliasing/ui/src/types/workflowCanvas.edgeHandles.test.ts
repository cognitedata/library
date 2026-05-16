import { describe, expect, it } from "vitest";
import {
  normalizeWorkflowCanvasEdgeHandles,
  parseWorkflowCanvasDocument,
} from "./workflowCanvas";

describe("normalizeWorkflowCanvasEdgeHandles", () => {
  it("sets validation branch for extraction → match_validation_extraction when handles are missing", () => {
    const nodes = [
      { id: "ext_a", kind: "extraction" as const, position: { x: 0, y: 0 }, data: {} },
      {
        id: "vr",
        kind: "match_validation_extraction" as const,
        position: { x: 0, y: 0 },
        data: {},
      },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      { id: "e1", source: "ext_a", target: "vr", kind: "data" },
    ]);
    expect(edges[0]?.source_handle).toBe("validation");
    expect(edges[0]?.target_handle).toBe("in");
  });

  it("coerces validate → match_validation_extraction to out (no validation source handle)", () => {
    const nodes = [
      { id: "va", kind: "validation" as const, position: { x: 0, y: 0 }, data: {} },
      {
        id: "vr",
        kind: "match_validation_extraction" as const,
        position: { x: 0, y: 0 },
        data: {},
      },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      { id: "e1", source: "va", target: "vr", kind: "data" },
    ]);
    expect(edges[0]?.source_handle).toBe("out");
    expect(edges[0]?.target_handle).toBe("in");
  });

  it("migrates legacy validate validation handle to out for match rule target", () => {
    const nodes = [
      { id: "va", kind: "validation" as const, position: { x: 0, y: 0 }, data: {} },
      {
        id: "vr",
        kind: "match_validation_extraction" as const,
        position: { x: 0, y: 0 },
        data: {},
      },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      {
        id: "e1",
        source: "va",
        target: "vr",
        kind: "data",
        source_handle: "validation",
        target_handle: "in",
      },
    ]);
    expect(edges[0]?.source_handle).toBe("out");
  });

  it("coerces mistaken extraction out handle into validation for match rule target", () => {
    const nodes = [
      { id: "ext_a", kind: "extraction" as const, position: { x: 0, y: 0 }, data: {} },
      {
        id: "vr",
        kind: "match_validation_extraction" as const,
        position: { x: 0, y: 0 },
        data: {},
      },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      { id: "e1", source: "ext_a", target: "vr", kind: "data", source_handle: "out", target_handle: "in" },
    ]);
    expect(edges[0]?.source_handle).toBe("validation");
  });

  it("does not rewrite subgraph port source handles on extraction", () => {
    const nodes = [
      { id: "ext_a", kind: "extraction" as const, position: { x: 0, y: 0 }, data: {} },
      { id: "end", kind: "end" as const, position: { x: 0, y: 0 }, data: {} },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      {
        id: "e1",
        source: "ext_a",
        target: "end",
        kind: "data",
        source_handle: "out__port1",
        target_handle: "in",
      },
    ]);
    expect(edges[0]?.source_handle).toBe("out__port1");
  });

  it("fills out/in for source_view → extraction when both handles are absent", () => {
    const nodes = [
      { id: "sv", kind: "source_view" as const, position: { x: 0, y: 0 }, data: {} },
      { id: "ext_a", kind: "extraction" as const, position: { x: 0, y: 0 }, data: {} },
    ];
    const edges = normalizeWorkflowCanvasEdgeHandles(nodes, [
      { id: "e1", source: "sv", target: "ext_a", kind: "data" },
    ]);
    expect(edges[0]?.source_handle).toBe("out");
    expect(edges[0]?.target_handle).toBe("in");
  });
});

describe("parseWorkflowCanvasDocument edge handle migration", () => {
  it("strips legacy extraction out_* handles then normalizes primary out → end", () => {
    const doc = parseWorkflowCanvasDocument({
      schemaVersion: 1,
      nodes: [
        { id: "a", kind: "extraction", position: { x: 0, y: 0 }, data: {} },
        { id: "b", kind: "end", position: { x: 0, y: 0 }, data: {} },
      ],
      edges: [
        { id: "e1", source: "a", target: "b", kind: "data", source_handle: "out_legacy", target_handle: "in" },
        { id: "e2", source: "a", target: "b", kind: "data", source_handle: "out__in", target_handle: "in" },
      ],
    });
    const byId = (id: string) => doc.edges.find((x) => x.id === id);
    expect(byId("e1")?.source_handle).toBe("out");
    expect(byId("e2")?.source_handle).toBe("out__in");
  });

  it("synthesizes stable edge ids when id is omitted (scope YAML style)", () => {
    const doc = parseWorkflowCanvasDocument({
      schemaVersion: 1,
      nodes: [
        { id: "st", kind: "start", position: { x: 0, y: 0 }, data: {} },
        {
          id: "vq",
          kind: "query_view",
          position: { x: 0, y: 0 },
          data: {
            config: {
              description: "q",
              view_space: "cdf_cdm",
              view_external_id: "CogniteFile",
              view_version: "v1",
            },
          },
        },
        {
          id: "va",
          kind: "validation",
          position: { x: 0, y: 0 },
          data: { config: { description: "v" } },
        },
        { id: "ii", kind: "inverted_index", position: { x: 0, y: 0 }, data: {} },
      ],
      edges: [
        { source: "st", target: "vq" },
        { source: "vq", target: "va" },
        { source: "va", target: "ii" },
      ],
    });
    expect(doc.edges).toHaveLength(3);
    expect(doc.edges.map((e) => e.id)).toEqual(["e_st_to_vq", "e_vq_to_va", "e_va_to_ii"]);
    expect(doc.edges[2]?.source_handle).toBe("out");
    expect(doc.edges[2]?.target_handle).toBe("in");
  });

  it("normalizes nested inner_canvas edges", () => {
    const doc = parseWorkflowCanvasDocument({
      schemaVersion: 1,
      nodes: [
        {
          id: "sg",
          kind: "subgraph",
          position: { x: 0, y: 0 },
          data: {
            inner_canvas: {
              schemaVersion: 1,
              nodes: [
                { id: "ix", kind: "extraction", position: { x: 0, y: 0 }, data: {} },
                { id: "iv", kind: "match_validation_extraction", position: { x: 0, y: 0 }, data: {} },
              ],
              edges: [{ id: "ie", source: "ix", target: "iv", kind: "data" }],
            },
          },
        },
      ],
      edges: [],
    });
    const inner = doc.nodes[0]?.data.inner_canvas?.edges?.[0];
    expect(inner?.source_handle).toBe("validation");
    expect(inner?.target_handle).toBe("in");
  });
});
