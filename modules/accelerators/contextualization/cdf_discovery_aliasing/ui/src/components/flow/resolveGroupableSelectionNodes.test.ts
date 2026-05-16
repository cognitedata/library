import type { Node } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { resolveGroupableSelectionNodes } from "./subflowMembership";

describe("resolveGroupableSelectionNodes", () => {
  it("includes box-selected ids from rf snapshot even when node.selected is false", () => {
    const a: Node = { id: "a", type: "keaExtraction", position: { x: 0, y: 0 }, data: {}, selected: false };
    const b: Node = { id: "b", type: "keaAliasing", position: { x: 1, y: 1 }, data: {}, selected: false };
    const c: Node = { id: "c", type: "keaSourceView", position: { x: 2, y: 2 }, data: {}, selected: false };
    const nodes = [a, b, c];
    const rfSnap = [
      { id: "a", type: "keaExtraction", position: { x: 0, y: 0 }, data: {} },
      { id: "b", type: "keaAliasing", position: { x: 1, y: 1 }, data: {} },
    ] as Node[];
    const out = resolveGroupableSelectionNodes(nodes, a, rfSnap);
    expect(out.map((n) => n.id).sort()).toEqual(["a", "b"]);
  });

  it("still honors node.selected when rf snapshot is empty", () => {
    const a: Node = { id: "a", type: "keaExtraction", position: { x: 0, y: 0 }, data: {}, selected: true };
    const b: Node = { id: "b", type: "keaAliasing", position: { x: 1, y: 1 }, data: {}, selected: true };
    const nodes = [a, b];
    const out = resolveGroupableSelectionNodes(nodes, a, []);
    expect(out.map((n) => n.id).sort()).toEqual(["a", "b"]);
  });
});
