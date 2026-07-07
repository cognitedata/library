/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import type { TreeNode } from "../types/discoveryNodes";
import {
  ancestorChainTo,
  buildBreadcrumbTrail,
  buildParentIndex,
  parentNodeId,
  treeNodeDrillable,
} from "./treeFilter";

function node(id: string, has_children: boolean, label = id): TreeNode {
  return { id, label, kind: "folder", has_children };
}

describe("parent index navigation", () => {
  const dmTree = new Map<string, TreeNode[]>([
    ["connection", [node("data", true)]],
    ["data", [node("dm", true, "Data Models")]],
    [
      "dm",
      [node("dm:model:cdf_cdm:CogniteCore:v1", true, "CogniteCore (v1)")],
    ],
  ]);
  const parentIndex = buildParentIndex(dmTree);

  it("resolves parent for compound data-model ids", () => {
    expect(parentNodeId("dm:model:cdf_cdm:CogniteCore:v1", "connection", parentIndex)).toBe("dm");
    expect(parentNodeId("dm", "connection", parentIndex)).toBe("data");
  });

  it("builds ancestor chain without phantom colon segments", () => {
    expect(ancestorChainTo("dm:model:cdf_cdm:CogniteCore:v1", "connection", parentIndex)).toEqual([
      "connection",
      "data",
      "dm",
      "dm:model:cdf_cdm:CogniteCore:v1",
    ]);
  });

  it("uses node labels in breadcrumbs instead of raw ids", () => {
    const root = node("connection", true, "my-project");
    const trail = buildBreadcrumbTrail(
      "dm:model:cdf_cdm:CogniteCore:v1",
      dmTree,
      root,
      (n) => n.label
    );
    expect(trail.map((s) => s.label)).toEqual([
      "my-project",
      "data",
      "Data Models",
      "CogniteCore (v1)",
    ]);
  });
});

describe("treeNodeDrillable", () => {
  it("returns false for leaves and loading placeholders", () => {
    const map = new Map<string, TreeNode[]>();
    const loaded = new Set<string>();
    expect(treeNodeDrillable(node("leaf", false), map, loaded)).toBe(false);
    expect(
      treeNodeDrillable(
        { id: "x:__loading__", label: "…", kind: "loading", has_children: false },
        map,
        loaded
      )
    ).toBe(false);
  });

  it("returns true before children are loaded", () => {
    const map = new Map<string, TreeNode[]>();
    const loaded = new Set<string>();
    expect(treeNodeDrillable(node("folder", true), map, loaded)).toBe(true);
  });

  it("returns false when loaded children are empty", () => {
    const folder = node("folder", true);
    const map = new Map<string, TreeNode[]>([["folder", []]]);
    const loaded = new Set(["folder"]);
    expect(treeNodeDrillable(folder, map, loaded)).toBe(false);
  });

  it("returns true when loaded children exist", () => {
    const folder = node("folder", true);
    const child = node("child", false);
    const map = new Map<string, TreeNode[]>([["folder", [child]]]);
    const loaded = new Set(["folder"]);
    expect(treeNodeDrillable(folder, map, loaded)).toBe(true);
  });

  it("ignores loading placeholder children when checking emptiness", () => {
    const folder = node("folder", true);
    const loading = {
      id: "folder:__loading__",
      label: "Loading…",
      kind: "loading",
      has_children: false,
    };
    const map = new Map<string, TreeNode[]>([["folder", [loading]]]);
    const loaded = new Set(["folder"]);
    expect(treeNodeDrillable(folder, map, loaded)).toBe(false);
  });
});
