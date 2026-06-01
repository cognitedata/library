import { describe, expect, it } from "vitest";
import {
  layoutDmViewNodesByMethod,
  layoutDmViewNodesDagre,
  layoutDmViewNodesForce,
  layoutDmViewNodesGrid,
} from "./dataModelFlowLayout";

describe("layoutDmViewNodesByMethod", () => {
  const nodes = [
    { id: "a", type: "dmView", position: { x: 0, y: 0 }, data: {} },
    { id: "b", type: "dmView", position: { x: 0, y: 0 }, data: {} },
    { id: "c", type: "dmView", position: { x: 0, y: 0 }, data: {} },
  ];
  const edges = [
    { id: "e1", source: "a", target: "b" },
    { id: "e2", source: "b", target: "c" },
  ];

  it("grid places views in increasing x within each row", () => {
    const laidOut = layoutDmViewNodesGrid(nodes);
    expect(laidOut[0]!.position.x).toBeLessThan(laidOut[1]!.position.x);
  });

  it("dagre orders views along relation edges left-to-right", () => {
    const laidOut = layoutDmViewNodesDagre(nodes, edges, "lr");
    const a = laidOut.find((n) => n.id === "a")!;
    const c = laidOut.find((n) => n.id === "c")!;
    expect(c.position.x).toBeGreaterThan(a.position.x);
  });

  it("force layout separates connected nodes for graph readability", () => {
    const laidOut = layoutDmViewNodesForce(nodes, edges);
    const a = laidOut.find((n) => n.id === "a")!;
    const b = laidOut.find((n) => n.id === "b")!;
    const c = laidOut.find((n) => n.id === "c")!;
    expect(Number.isFinite(a.position.x)).toBe(true);
    expect(Number.isFinite(b.position.y)).toBe(true);
    const ab = Math.hypot(a.position.x - b.position.x, a.position.y - b.position.y);
    const bc = Math.hypot(b.position.x - c.position.x, b.position.y - c.position.y);
    expect(ab).toBeGreaterThan(20);
    expect(bc).toBeGreaterThan(20);
  });

  it("grid method ignores relation edges", () => {
    const laidOut = layoutDmViewNodesByMethod(nodes, edges, "lr", "grid");
    expect(laidOut[0]!.position).toEqual(layoutDmViewNodesGrid(nodes)[0]!.position);
  });

  it("force method produces a non-grid graph layout", () => {
    const force = layoutDmViewNodesByMethod(nodes, edges, "lr", "force");
    const grid = layoutDmViewNodesGrid(nodes);
    expect(force[1]!.position).not.toEqual(grid[1]!.position);
  });
});
