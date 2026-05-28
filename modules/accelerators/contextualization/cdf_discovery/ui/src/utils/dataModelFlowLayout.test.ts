import { describe, expect, it } from "vitest";
import {
  layoutDmViewNodesByMethod,
  layoutDmViewNodesDagre,
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

  it("grid method ignores relation edges", () => {
    const laidOut = layoutDmViewNodesByMethod(nodes, edges, "lr", "grid");
    expect(laidOut[0]!.position).toEqual(layoutDmViewNodesGrid(nodes)[0]!.position);
  });
});
