import type { Edge, Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { materializePaletteDrop } from "./materializePaletteDrop";

describe("materializePaletteDrop", () => {
  const base = {
    position: { x: 10, y: 10 },
    edges: [] as Edge[],
    workflowScopeDoc: {} as Record<string, unknown>,
    patchWorkflowScope: (fn: (doc: Record<string, unknown>) => Record<string, unknown>) => {
      void fn({});
    },
    t: (key: string) => key,
  };

  it("appends auto data edge from dropped view save to first discoveryEnd", () => {
    const end: Node = { id: "end1", type: "discoveryEnd", position: { x: 0, y: 0 }, data: {} };
    const r = materializePaletteDrop({
      ...base,
      payload: { kind: "discovery", stage: "save_view" },
      nodes: [end],
    });
    expect(r.outcome).toBe("create");
    if (r.outcome !== "create") return;
    const toEnd = r.extraEdges.find((e) => e.target === "end1");
    expect(toEnd).toBeDefined();
    expect(toEnd?.sourceHandle).toBe("out");
    expect(toEnd?.targetHandle).toBe("in");
    expect((toEnd?.data as { kind?: string } | undefined)?.kind).toBe("data");
  });

  it("appends auto data edge from dropped inverted index to first discoveryEnd", () => {
    const end: Node = { id: "end1", type: "discoveryEnd", position: { x: 0, y: 0 }, data: {} };
    const r = materializePaletteDrop({
      ...base,
      payload: { kind: "discovery", stage: "inverted_index" },
      nodes: [end],
    });
    expect(r.outcome).toBe("create");
    if (r.outcome !== "create") return;
    const toEnd = r.extraEdges.find((e) => e.target === "end1");
    expect(toEnd).toBeDefined();
    expect(toEnd?.sourceHandle).toBe("out");
    expect(toEnd?.targetHandle).toBe("in");
  });

  it("does not append persistence→end edge when canvas has no discoveryEnd", () => {
    const r = materializePaletteDrop({
      ...base,
      payload: { kind: "discovery", stage: "save_raw" },
      nodes: [],
    });
    expect(r.outcome).toBe("create");
    if (r.outcome !== "create") return;
    expect(r.extraEdges).toEqual([]);
  });
});
