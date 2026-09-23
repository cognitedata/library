import { describe, expect, it } from "vitest";
import {
  parseScoringRuleRows,
  serializeScoringRuleRows,
  type ScoringRuleRow,
} from "./scoreNodeConfigModel";

function roundTrip(rows: ScoringRuleRow[]): ScoringRuleRow[] {
  return parseScoringRuleRows(serializeScoringRuleRows(rows));
}

describe("scoreNodeConfigModel scoring-rule expressions", () => {
  it("seeds one blank expression when a rule has an empty expressions list", () => {
    const rows = parseScoringRuleRows([
      { name: "blacklist", enabled: true, match: { expressions: [], keywords: ["test"] } },
    ]);
    expect(rows[0]?.expressions).toEqual([{ pattern: "", description: "" }]);
  });

  it("does not persist the injected placeholder when serializing a single blank row", () => {
    const rows = parseScoringRuleRows([
      { name: "blacklist", enabled: true, match: { expressions: [], keywords: ["test"] } },
    ]);
    const serialized = serializeScoringRuleRows(rows);
    const match = serialized[0]?.match as { expressions?: unknown[] };
    expect(match.expressions).toEqual([]);
  });

  it("round-trips a newly added blank expression on a rule that started empty", () => {
    const rows = parseScoringRuleRows([
      { name: "blacklist", enabled: true, match: { expressions: [], keywords: ["test"] } },
    ]);
    const withAdded: ScoringRuleRow[] = [
      {
        ...rows[0]!,
        expressions: [...rows[0]!.expressions, { pattern: "", description: "" }],
      },
    ];
    const again = roundTrip(withAdded);
    expect(again[0]?.expressions).toHaveLength(2);
    expect(again[0]?.expressions).toEqual([
      { pattern: "", description: "" },
      { pattern: "", description: "" },
    ]);
  });

  it("round-trips a trailing space in description without stripping it", () => {
    const rows = parseScoringRuleRows([
      {
        name: "isa_compliant",
        enabled: true,
        match: { expressions: [{ pattern: String.raw`\bP\b`, description: "pump" }] },
      },
    ]);
    const withSpace: ScoringRuleRow[] = [
      {
        ...rows[0]!,
        expressions: [{ pattern: String.raw`\bP\b`, description: "pump " }],
      },
    ];
    const again = roundTrip(withSpace);
    expect(again[0]?.expressions[0]?.description).toBe("pump ");
  });

  it("round-trips a description typed on a blank pattern row", () => {
    const rows = parseScoringRuleRows([
      { name: "blacklist", enabled: true, match: { expressions: [], keywords: ["test"] } },
    ]);
    const withDesc: ScoringRuleRow[] = [
      {
        ...rows[0]!,
        expressions: [{ pattern: "", description: "custom note " }],
      },
    ];
    const again = roundTrip(withDesc);
    expect(again[0]?.expressions[0]).toEqual({ pattern: "", description: "custom note " });
  });

  it("round-trips a newly added blank expression next to existing patterns", () => {
    const rows = parseScoringRuleRows([
      {
        name: "isa_compliant",
        enabled: true,
        match: { expressions: [{ pattern: String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`, description: "pump" }] },
      },
    ]);
    const withAdded: ScoringRuleRow[] = [
      {
        ...rows[0]!,
        expressions: [...rows[0]!.expressions, { pattern: "", description: "" }],
      },
    ];
    const again = roundTrip(withAdded);
    expect(again[0]?.expressions).toHaveLength(2);
    expect(again[0]?.expressions[1]).toEqual({ pattern: "", description: "" });
    expect(again[0]?.expressions[0]?.pattern).toBe(String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`);
  });
});
