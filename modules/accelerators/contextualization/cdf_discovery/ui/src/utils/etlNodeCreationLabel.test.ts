import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import {
  applyWiredCreationLabel,
  composeWiredEtlNodeLabel,
} from "./etlNodeCreationLabel";

const t = (key: string) =>
  (
    {
      "transform.palette.filter": "Filter",
      "transform.palette.query_view": "View query",
      "transforms.handlerName.trim_whitespace": "Trim whitespace",
    } as Record<string, string>
  )[key] ?? key;

describe("etlNodeCreationLabel", () => {
  it("composes type and predecessor labels", () => {
    const pred: Node = {
      id: "q1",
      type: "etlQueryView",
      position: { x: 0, y: 0 },
      data: { kind: "query_view", label: "Assets" },
    };
    expect(composeWiredEtlNodeLabel("filter", pred, t)).toBe("Filter · Assets");
    expect(composeWiredEtlNodeLabel("transform", pred, t, "trim_whitespace")).toBe(
      "Trim whitespace · Assets"
    );
  });

  it("uses display fallback for predecessor without label", () => {
    const pred: Node = {
      id: "q1",
      type: "etlQueryView",
      position: { x: 0, y: 0 },
      data: { kind: "query_view" },
    };
    expect(composeWiredEtlNodeLabel("filter", pred, t)).toBe("Filter · View query");
  });

  it("applyWiredCreationLabel sets label on node data", () => {
    const pred: Node = {
      id: "q1",
      type: "etlQueryView",
      position: { x: 0, y: 0 },
      data: { kind: "query_view", label: "Upstream" },
    };
    const node: Node = {
      id: "f1",
      type: "etlFilter",
      position: { x: 0, y: 0 },
      data: { kind: "filter", config: { description: "" } },
    };
    const labeled = applyWiredCreationLabel(node, pred, t);
    expect((labeled.data as { label?: string }).label).toBe("Filter · Upstream");
    expect((labeled.data as { config?: { description?: string } }).config?.description).toBe(
      "Filter · Upstream"
    );
  });

  it("does not overwrite an existing label", () => {
    const pred: Node = {
      id: "q1",
      type: "etlQueryView",
      position: { x: 0, y: 0 },
      data: { kind: "query_view", label: "Upstream" },
    };
    const node: Node = {
      id: "f1",
      type: "etlFilter",
      position: { x: 0, y: 0 },
      data: { kind: "filter", label: "Custom" },
    };
    const labeled = applyWiredCreationLabel(node, pred, t);
    expect((labeled.data as { label?: string }).label).toBe("Custom");
  });
});
