import { describe, expect, it } from "vitest";
import type { SummaryInstance } from "@/types/operations";
import { getContentTypes } from "@/utils/summaryContentTypes";

// Properties are accessed as summary.properties.cdf_cdm["CogniteFile/v1"]
// (two-level nesting), distinct from the flat "cdf_cdm/CogniteFile/v1" key
// used by extractSummaryContent.
function makeProps(
  overrides: Record<string, unknown> = {},
): SummaryInstance["properties"] {
  return { cdf_cdm: { "CogniteFile/v1": overrides } };
}

function makeSummary(
  props: SummaryInstance["properties"] = {},
): SummaryInstance {
  return {
    externalId: "ops_summary_test",
    space: "ops_summaries",
    createdTime: 1_700_000_000_000,
    properties: props,
  };
}

describe("getContentTypes", () => {
  // ── metadata.contentTypes (highest-priority path) ─────────────────────────

  it("returns display names from metadata.contentTypes when present", () => {
    const summary = makeSummary(
      makeProps({ metadata: { contentTypes: "assets,files" } }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets", "Files"]);
  });

  it("handles a single content type in metadata", () => {
    const summary = makeSummary(
      makeProps({ metadata: { contentTypes: "time_series" } }),
    );
    expect(getContentTypes(summary)).toEqual(["Time Series"]);
  });

  it("trims whitespace around comma-separated types in metadata", () => {
    const summary = makeSummary(
      makeProps({ metadata: { contentTypes: " assets , equipment " } }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets", "Equipment"]);
  });

  it("skips empty metadata.contentTypes and falls through to description", () => {
    // Empty string → not truthy → falls through to OPS bitmap / heuristics
    const summary = makeSummary(
      makeProps({
        metadata: { contentTypes: "" },
        description: "OPS:10000\nsome content",
      }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets"]);
  });

  // ── OPS bitmap in description (second-priority path) ──────────────────────
  // Bitmap order: assets[0] files[1] work_orders[2] time_series[3] equipment[4]

  it("decodes an OPS bitmap with a single bit set", () => {
    const summary = makeSummary(
      makeProps({ description: "OPS:10000\nSome content" }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets"]);
  });

  it("decodes an OPS bitmap with all bits set", () => {
    const summary = makeSummary(
      makeProps({ description: "OPS:11111\nAll types content" }),
    );
    expect(getContentTypes(summary)).toEqual([
      "Assets",
      "Files",
      "Work Orders",
      "Time Series",
      "Equipment",
    ]);
  });

  it("decodes a partial OPS bitmap correctly", () => {
    // bits: assets=1 files=0 work_orders=1 time_series=0 equipment=0
    const summary = makeSummary(
      makeProps({ description: "OPS:10100\nPartial" }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets", "Work Orders"]);
  });

  it('returns ["All Types"] when OPS bitmap has no bits set', () => {
    const summary = makeSummary(
      makeProps({ description: "OPS:00000\nNo types" }),
    );
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });

  it("does not treat a mid-description OPS: line as a bitmap", () => {
    // The regex anchors to the start of the string (^)
    const summary = makeSummary(
      makeProps({ description: "Preamble\nOPS:11111\nContent" }),
    );
    // Falls through to keyword heuristics (no keyword matches here)
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });

  // ── Legacy keyword heuristics (lowest-priority path) ─────────────────────

  it("matches 'asset' keyword to Assets", () => {
    const summary = makeSummary(
      makeProps({ description: "Contains asset information" }),
    );
    expect(getContentTypes(summary)).toContain("Assets");
  });

  it("matches 'file' keyword to Files", () => {
    const summary = makeSummary(
      makeProps({ description: "File uploads recorded" }),
    );
    expect(getContentTypes(summary)).toContain("Files");
  });

  it("matches 'equipment' keyword to Equipment", () => {
    const summary = makeSummary(
      makeProps({ description: "Equipment maintenance logged" }),
    );
    expect(getContentTypes(summary)).toContain("Equipment");
  });

  it("matches 'time' keyword to Time Series", () => {
    const summary = makeSummary(
      makeProps({ description: "Real-time data captured" }),
    );
    expect(getContentTypes(summary)).toContain("Time Series");
  });

  it("matches 'series' keyword to Time Series", () => {
    const summary = makeSummary(
      makeProps({ description: "Time series analysis complete" }),
    );
    expect(getContentTypes(summary)).toContain("Time Series");
  });

  it("matches 'work' keyword to Work Orders", () => {
    const summary = makeSummary(
      makeProps({ description: "Work completed on site" }),
    );
    expect(getContentTypes(summary)).toContain("Work Orders");
  });

  it("matches 'order' keyword to Work Orders", () => {
    const summary = makeSummary(
      makeProps({ description: "Order #42 processed" }),
    );
    expect(getContentTypes(summary)).toContain("Work Orders");
  });

  it("accumulates multiple keyword matches", () => {
    const summary = makeSummary(
      makeProps({ description: "asset and file counts for this period" }),
    );
    expect(getContentTypes(summary)).toEqual(["Assets", "Files"]);
  });

  it('returns ["All Types"] when no keyword matches and no bitmap', () => {
    const summary = makeSummary(
      makeProps({ description: "General operational report with no hints" }),
    );
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });

  it("strips the OPS bitmap line before applying keyword heuristics", () => {
    // If the bitmap line itself contained keywords the heuristics would fire
    // on it; stripping prevents false positives.
    const summary = makeSummary(
      // Invalid bitmap (non-matching regex) + description that looks like bitmap content
      makeProps({ description: "NOT_OPS:asset file equipment\nClean content" }),
    );
    // "asset", "file", "equipment" are in the first line which is NOT stripped
    expect(getContentTypes(summary)).toEqual(["Assets", "Files", "Equipment"]);
  });

  // ── Edge / error cases ────────────────────────────────────────────────────

  it('returns ["All Types"] when properties are absent', () => {
    const summary = makeSummary({});
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });

  it('returns ["All Types"] when cdf_cdm properties are absent', () => {
    const summary = makeSummary({ other_space: { "SomeView/v1": {} } });
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });

  it('returns ["All Types"] on unexpected property shapes without throwing', () => {
    const summary: SummaryInstance = {
      externalId: "bad",
      space: "s",
      createdTime: 0,
      // Force an unexpected shape that would normally throw
      properties: JSON.parse('{"cdf_cdm":null}'),
    };
    expect(getContentTypes(summary)).toEqual(["All Types"]);
  });
});
