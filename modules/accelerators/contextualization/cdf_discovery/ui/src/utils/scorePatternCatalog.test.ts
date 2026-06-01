import { describe, expect, it } from "vitest";
import {
  enrichExpressionDescriptions,
  lookupScorePatternDescription,
  normalizeScorePatternKey,
  withOptionalIsaTagAreaPrefix,
} from "./scorePatternCatalog";

describe("scorePatternCatalog", () => {
  it("normalizes pattern keys", () => {
    expect(normalizeScorePatternKey("  \\bP[-_]?\\d{1,6}\\b  ")).toBe("\\bP[-_]?\\d{1,6}\\b");
  });

  it("looks up ISA pump pattern from aliasing catalog", () => {
    const desc = lookupScorePatternDescription(String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`);
    expect(desc).toBe("Standard pump tags (P-101, P101A, P-2001)");
  });

  it("looks up alias shape invalid pattern", () => {
    const desc = lookupScorePatternDescription("^[0-9]{0,3}$");
    expect(desc).toContain("digits");
  });

  it("withOptionalIsaTagAreaPrefix wraps ISA suffix patterns", () => {
    const wrapped = withOptionalIsaTagAreaPrefix(String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`);
    expect(wrapped).toContain(String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`);
    expect(wrapped.length).toBeGreaterThan(String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`.length);
  });

  it("enrichExpressionDescriptions fills empty descriptions only", () => {
    const out = enrichExpressionDescriptions([
      { pattern: String.raw`\bP[-_]?\d{1,6}[A-Z]?\b`, description: "" },
      { pattern: "^custom$", description: "keep me" },
    ]);
    expect(out[0]?.description).toBe("Standard pump tags (P-101, P101A, P-2001)");
    expect(out[1]?.description).toBe("keep me");
  });
});
