import { describe, expect, it } from "vitest";
import {
  appendSourceViewToExtractionAssociation,
  applySourceViewExtractionAssociationsFromCanvas,
  collectSourceViewToExtractionPairsFromCanvas,
  hasWorkflowAssociationsKey,
  mergeSourceViewToExtractionAssociationsIntoDoc,
  parseSourceViewToExtractionPairs,
} from "./workflowScopeAssociations";

describe("workflowScopeAssociations", () => {
  it("collects source_view → extraction pairs from data edges", () => {
    const canvas = {
      schemaVersion: 1,
      nodes: [
        {
          id: "sv_0",
          kind: "source_view" as const,
          position: { x: 0, y: 0 },
          data: { ref: { source_view_index: 0 } },
        },
        {
          id: "ext_a",
          kind: "extraction" as const,
          position: { x: 1, y: 0 },
          data: { ref: { extraction_rule_name: "rule_a" } },
        },
      ],
      edges: [
        {
          id: "e1",
          source: "sv_0",
          target: "ext_a",
          kind: "data" as const,
          source_handle: "out",
          target_handle: "in",
        },
      ],
    };
    const pairs = collectSourceViewToExtractionPairsFromCanvas(canvas);
    expect(pairs).toEqual([{ source_view_index: 0, extraction_rule_name: "rule_a" }]);
  });

  it("appendSourceViewToExtractionAssociation dedupes", () => {
    const doc: Record<string, unknown> = {
      associations: [
        { kind: "source_view_to_extraction", source_view_index: 0, extraction_rule_name: "r1" },
      ],
    };
    const once = appendSourceViewToExtractionAssociation(doc, {
      source_view_index: 0,
      extraction_rule_name: "r1",
    });
    expect(parseSourceViewToExtractionPairs(once)).toHaveLength(1);
    const twice = appendSourceViewToExtractionAssociation(once, {
      source_view_index: 0,
      extraction_rule_name: "r1",
    });
    expect(parseSourceViewToExtractionPairs(twice)).toHaveLength(1);
  });

  it("parses and dedupes associations from scope doc", () => {
    const doc: Record<string, unknown> = {
      associations: [
        { kind: "source_view_to_extraction", source_view_index: 1, extraction_rule_name: "x" },
        { kind: "source_view_to_extraction", source_view_index: 1, extraction_rule_name: "x" },
        { kind: "other", foo: 1 },
      ],
    };
    expect(parseSourceViewToExtractionPairs(doc)).toEqual([
      { source_view_index: 1, extraction_rule_name: "x" },
    ]);
    expect(mergeSourceViewToExtractionAssociationsIntoDoc(doc, [{ source_view_index: 0, extraction_rule_name: "y" }]))
      .toMatchObject({
        associations: expect.arrayContaining([
          { kind: "other", foo: 1 },
          { kind: "source_view_to_extraction", source_view_index: 0, extraction_rule_name: "y" },
        ]),
      });
  });

  it("does not add associations key when canvas has no bindings and doc never had associations", () => {
    const doc: Record<string, unknown> = {
      source_views: [{ view_external_id: "v" }],
      key_extraction: { config: { data: { extraction_rules: [{ name: "r1", scope_filters: { entity_type: ["v"] } }] } } },
    };
    const canvas = { schemaVersion: 1, nodes: [], edges: [] };
    const next = applySourceViewExtractionAssociationsFromCanvas(canvas, doc);
    expect(hasWorkflowAssociationsKey(next)).toBe(false);
  });

  it("merges source_view_to_extraction pairs from canvas without mutating scope_filters.entity_type", () => {
    const doc: Record<string, unknown> = {
      source_views: [
        { view_external_id: "alpha_view", entity_type: "asset" },
        { view_external_id: "beta_view", entity_type: "timeseries" },
      ],
      key_extraction: {
        config: {
          data: {
            extraction_rules: [{ name: "ext1", scope_filters: { entity_type: ["legacy"] } }],
          },
        },
      },
    };
    const canvas = {
      schemaVersion: 1,
      nodes: [
        {
          id: "sv_0",
          kind: "source_view" as const,
          position: { x: 0, y: 0 },
          data: { ref: { source_view_index: 0 } },
        },
        {
          id: "sv_1",
          kind: "source_view" as const,
          position: { x: 0, y: 1 },
          data: { ref: { source_view_index: 1 } },
        },
        {
          id: "ext1",
          kind: "extraction" as const,
          position: { x: 1, y: 0 },
          data: { ref: { extraction_rule_name: "ext1" } },
        },
      ],
      edges: [
        {
          id: "e0",
          source: "sv_0",
          target: "ext1",
          kind: "data" as const,
          source_handle: "out",
          target_handle: "in",
        },
        {
          id: "e1",
          source: "sv_1",
          target: "ext1",
          kind: "data" as const,
          source_handle: "out",
          target_handle: "in",
        },
      ],
    };
    const next = applySourceViewExtractionAssociationsFromCanvas(canvas, doc);
    expect(parseSourceViewToExtractionPairs(next).length).toBe(2);
    const ke = next.key_extraction as Record<string, unknown>;
    const rules = (ke.config as Record<string, unknown>).data as Record<string, unknown>;
    const row = (rules.extraction_rules as unknown[])[0] as Record<string, unknown>;
    expect(row.scope_filters).toEqual({ entity_type: ["legacy"] });
  });
});
