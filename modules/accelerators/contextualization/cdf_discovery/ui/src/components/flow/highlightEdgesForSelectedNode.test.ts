import { describe, expect, it } from "vitest";
import {
  FLOW_EDGE_CONNECTED_CLASS,
  highlightEdgesConnectedToNode,
} from "./highlightEdgesForSelectedNode";

describe("highlightEdgesConnectedToNode", () => {
  const edges = [
    { id: "a-b", source: "a", target: "b" },
    { id: "b-c", source: "b", target: "c" },
  ];

  it("adds connected class on incident edges", () => {
    const next = highlightEdgesConnectedToNode(edges, "b");
    expect(next[0]?.className).toContain(FLOW_EDGE_CONNECTED_CLASS);
    expect(next[1]?.className).toContain(FLOW_EDGE_CONNECTED_CLASS);
  });

  it("removes connected class when selection clears", () => {
    const highlighted = highlightEdgesConnectedToNode(edges, "b");
    const cleared = highlightEdgesConnectedToNode(highlighted, null);
    expect(cleared.every((e) => !e.className?.includes(FLOW_EDGE_CONNECTED_CLASS))).toBe(true);
  });
});
