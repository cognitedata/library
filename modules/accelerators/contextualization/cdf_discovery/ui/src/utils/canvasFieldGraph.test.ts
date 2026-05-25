import { describe, expect, it } from "vitest";
import type { Edge, Node } from "@xyflow/react";
import {
  collectPredecessorIds,
  collectSuccessorIds,
  inferFieldsEmittedByNode,
  inferFieldsRequiredByNode,
  parseSqlSelectColumns,
  resolveAvailableInputFields,
  resolveSuggestedOutputFields,
  seedMappingsBetweenNodes,
} from "./canvasFieldGraph";
import {
  buildConceptualSqlPreview,
  mergeMappingsWithSuggestions,
  parseFieldMappings,
} from "./fieldMapNodeConfigModel";

function node(id: string, kind: string, config: Record<string, unknown> = {}): Node {
  return {
    id,
    type: `etl${kind.charAt(0).toUpperCase()}${kind.slice(1).replace(/_([a-z])/g, (_, c) => c.toUpperCase())}`,
    position: { x: 0, y: 0 },
    data: { kind, config },
  };
}

function dataEdge(source: string, target: string): Edge {
  return { id: `${source}->${target}`, source, target, data: { kind: "data" } };
}

describe("parseSqlSelectColumns", () => {
  it("parses aliased columns", () => {
    expect(
      parseSqlSelectColumns(
        "SELECT externalId, name AS asset_name FROM cdf_nodes('s', 'A', 'v1')"
      )
    ).toEqual(["externalId", "asset_name"]);
  });

  it("returns empty for SELECT *", () => {
    expect(parseSqlSelectColumns("SELECT * FROM t")).toEqual([]);
  });
});

describe("canvasFieldGraph", () => {
  const queryView = node("q1", "query_view", {
    include_properties: ["name", "description"],
  });
  const transform = node("t1", "transform", {
    fields: [{ field_name: "indexKey" }],
    output_field: "aliases",
  });
  const fieldMap = node("m1", "field_map", {
    mappings: [{ input_field: "name", output_field: "indexKey" }],
  });
  const edges = [dataEdge("q1", "m1"), dataEdge("m1", "t1")];
  const nodes = [queryView, fieldMap, transform];

  it("collects predecessor and successor ids", () => {
    expect(collectPredecessorIds(nodes, edges, "m1")).toEqual(["q1"]);
    expect(collectSuccessorIds(nodes, edges, "m1")).toEqual(["t1"]);
  });

  it("infers emitted fields from query_view", () => {
    expect(inferFieldsEmittedByNode(queryView)).toEqual(["name", "description"]);
  });

  it("infers required fields from transform", () => {
    expect(inferFieldsRequiredByNode(transform)).toEqual(["indexKey"]);
  });

  it("resolves available input fields from predecessor", () => {
    const inputs = resolveAvailableInputFields({ nodes, edges, nodeId: "m1" });
    expect(inputs).toContain("name");
    expect(inputs).toContain("description");
  });

  it("resolves suggested output fields from successor", () => {
    const outputs = resolveSuggestedOutputFields({ nodes, edges, nodeId: "m1" });
    expect(outputs).toContain("indexKey");
    expect(outputs).not.toContain("aliases");
  });

  it("seeds mappings between connected nodes", () => {
    const rows = seedMappingsBetweenNodes(queryView, transform);
    expect(rows.some((r) => r.output_field === "indexKey")).toBe(true);
  });
});

describe("fieldMapNodeConfigModel", () => {
  it("parses mappings from config", () => {
    expect(
      parseFieldMappings({ mappings: [{ input_field: "a", output_field: "b" }] })
    ).toEqual([{ input_field: "a", output_field: "b" }]);
  });

  it("builds conceptual SQL preview", () => {
    const sql = buildConceptualSqlPreview(
      [{ input_field: "name", output_field: "indexKey" }],
      "cdf_nodes('s', 'A', 'v1')"
    );
    expect(sql).toContain("name AS indexKey");
    expect(sql).toContain("FROM cdf_nodes");
  });

  it("mergeMappingsWithSuggestions adds missing outputs", () => {
    const merged = mergeMappingsWithSuggestions(
      [{ input_field: "name", output_field: "indexKey" }],
      ["name", "description"],
      ["aliases"]
    );
    expect(merged).toHaveLength(2);
    expect(merged.some((r) => r.output_field === "aliases")).toBe(true);
  });
});
