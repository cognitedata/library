import { describe, expect, it } from "vitest";
import { appendAliasingRuleForHandler } from "./ensureAliasingRuleForPaletteDrop";

describe("appendAliasingRuleForHandler", () => {
  it("creates aliasing block and first rule for prefix_suffix", () => {
    const { doc, ruleName } = appendAliasingRuleForHandler({}, "prefix_suffix");
    expect(ruleName).toBe("prefix_suffix_1");
    const rules = (doc.aliasing as { config: { data: { aliasing_rules: unknown[] } } }).config.data
      .aliasing_rules;
    expect(rules).toHaveLength(1);
    const row = rules[0] as Record<string, unknown>;
    expect(row.name).toBe("prefix_suffix_1");
    expect(row.handler).toBe("prefix_suffix");
    expect(row.priority).toBe(10);
    expect(row.config).toMatchObject({ operation: "add_prefix" });
  });

  it("appends sequential names and priorities", () => {
    const first = appendAliasingRuleForHandler({}, "prefix_suffix");
    const second = appendAliasingRuleForHandler(first.doc, "character_substitution");
    expect(second.ruleName).toBe("character_substitution_1");
    const rules = (second.doc.aliasing as { config: { data: { aliasing_rules: unknown[] } } }).config.data
      .aliasing_rules;
    expect(rules).toHaveLength(2);
    expect((rules[1] as Record<string, unknown>).priority).toBe(20);
  });

  it("preserves existing aliasing.config when appending", () => {
    const doc0 = {
      aliasing: {
        externalId: "x",
        config: {
          parameters: { debug: true },
          data: {
            other: 1,
            aliasing_rules: [{ name: "aliasing_rule_1", handler: "composite", enabled: true }],
          },
        },
      },
    };
    const { doc, ruleName } = appendAliasingRuleForHandler(doc0, "prefix_suffix");
    expect(ruleName).toBe("prefix_suffix_1");
    expect((doc.aliasing as Record<string, unknown>).externalId).toBe("x");
    expect(
      ((doc.aliasing as Record<string, unknown>).config as Record<string, unknown>).parameters
    ).toEqual({ debug: true });
    const data = ((doc.aliasing as Record<string, unknown>).config as Record<string, unknown>).data as Record<
      string,
      unknown
    >;
    expect(data.other).toBe(1);
    expect(Array.isArray(data.aliasing_rules) && data.aliasing_rules.length).toBe(2);
  });
});
