import { describe, expect, it } from "vitest";
import type { Node } from "@xyflow/react";
import { wrapSelectionInNewSubflow } from "./wrapSelectionInSubflow";

describe("wrapSelectionInNewSubflow", () => {
  it("creates an organizational subflow (no I/O hubs) and parents one selected node", () => {
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 100, y: 200 },
      data: { label: "A" },
      selected: true,
    };
    const nodes: Node[] = [a];
    const out = wrapSelectionInNewSubflow(nodes, [a]);
    expect(out).not.toBeNull();
    const sf = out!.find((n) => n.type === "keaSubflow");
    expect(sf).toBeDefined();
    expect(String((sf!.data as { label?: string }).label)).toBe("Subflow");
    expect(out!.find((n) => n.type === "keaSubflowGraphIn")).toBeUndefined();
    expect(out!.find((n) => n.type === "keaSubflowGraphOut")).toBeUndefined();
    const na = out!.find((n) => n.id === "a");
    expect(na?.parentId).toBe(sf!.id);
  });

  it("creates a subflow and parents two selected nodes in a grid", () => {
    const a: Node = {
      id: "a",
      type: "keaExtraction",
      position: { x: 100, y: 200 },
      data: { label: "A" },
      selected: true,
    };
    const b: Node = {
      id: "b",
      type: "keaAliasing",
      position: { x: 300, y: 250 },
      data: { label: "B" },
      selected: true,
    };
    const nodes: Node[] = [a, b];
    const out = wrapSelectionInNewSubflow(nodes, [a, b]);
    expect(out).not.toBeNull();
    const sf = out!.find((n) => n.type === "keaSubflow");
    expect(sf).toBeDefined();
    expect(sf!.parentId).toBeUndefined();
    expect((sf!.style as { width: number }).width).toBeGreaterThanOrEqual(200);
    expect((sf!.style as { height: number }).height).toBeGreaterThanOrEqual(140);
    const na = out!.find((n) => n.id === "a");
    const nb = out!.find((n) => n.id === "b");
    expect(na?.parentId).toBe(sf!.id);
    expect(nb?.parentId).toBe(sf!.id);
    expect(na?.position.x).toBeGreaterThanOrEqual(20);
    expect(na?.position.y).toBeGreaterThanOrEqual(40);
  });
});
