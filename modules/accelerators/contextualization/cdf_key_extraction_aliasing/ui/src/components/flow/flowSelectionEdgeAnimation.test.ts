import { describe, expect, it } from "vitest";
import { upstreamDownstreamAnimatedEdgeIds } from "./flowSelectionEdgeAnimation";
import { runProgressAnimatedEdgeIds } from "./flowRunProgressEdges";

describe("upstreamDownstreamAnimatedEdgeIds", () => {
  const edges = [
    { id: "e_st_a", source: "st", target: "a" },
    { id: "e_a_b", source: "a", target: "b" },
    { id: "e_a_c", source: "a", target: "c" },
    { id: "e_b_end", source: "b", target: "end" },
  ];

  it("includes upstream and downstream of one selected node", () => {
    const ids = upstreamDownstreamAnimatedEdgeIds(edges, ["b"]);
    expect(ids.has("e_a_b")).toBe(true);
    expect(ids.has("e_b_end")).toBe(true);
    expect(ids.has("e_st_a")).toBe(true);
    expect(ids.has("e_a_c")).toBe(false);
  });

  it("returns empty when nothing selected", () => {
    expect(upstreamDownstreamAnimatedEdgeIds(edges, []).size).toBe(0);
  });

  it("unions neighborhoods for two seeds", () => {
    const ids = upstreamDownstreamAnimatedEdgeIds(edges, ["b", "c"]);
    expect(ids.has("e_a_c")).toBe(true);
    expect(ids.has("e_a_b")).toBe(true);
  });
});

describe("runProgressAnimatedEdgeIds", () => {
  const edges = [
    { id: "e1", target: "n1" },
    { id: "e2", target: "n2" },
  ];

  it("animates edges into active or completed nodes", () => {
    const ids = runProgressAnimatedEdgeIds(edges, ["n1"], ["n2"]);
    expect(ids.has("e1")).toBe(true);
    expect(ids.has("e2")).toBe(true);
  });

  it("returns empty when no hot nodes", () => {
    expect(runProgressAnimatedEdgeIds(edges, [], []).size).toBe(0);
  });
});
