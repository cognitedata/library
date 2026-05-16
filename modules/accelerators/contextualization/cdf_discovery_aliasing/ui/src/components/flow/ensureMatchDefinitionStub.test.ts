import { describe, expect, it } from "vitest";
import { confidenceMatchDefinitionsAsMap } from "../../utils/confidenceMatchDefinitionIds";
import {
  appendUniqueMatchDefinitionStub,
  ensureConfidenceMatchRuleDefinitionStub,
  nextUniqueMatchDefinitionId,
} from "./ensureMatchDefinitionStub";

describe("ensureMatchDefinitionStub", () => {
  it("appendUniqueMatchDefinitionStub inserts rule_1 on empty doc", () => {
    const { doc, newId } = appendUniqueMatchDefinitionStub({});
    expect(newId).toBe("rule_1");
    const map = confidenceMatchDefinitionsAsMap(doc);
    expect(Object.keys(map).sort()).toEqual(["rule_1"]);
    expect(map.rule_1?.name).toBe("rule_1");
  });

  it("appendUniqueMatchDefinitionStub picks next free id", () => {
    const first = appendUniqueMatchDefinitionStub({});
    const second = appendUniqueMatchDefinitionStub(first.doc);
    expect(second.newId).toBe("rule_2");
    expect(Object.keys(confidenceMatchDefinitionsAsMap(second.doc)).sort()).toEqual(["rule_1", "rule_2"]);
  });

  it("ensureConfidenceMatchRuleDefinitionStub is idempotent when id exists", () => {
    const seeded = appendUniqueMatchDefinitionStub({}).doc;
    const again = ensureConfidenceMatchRuleDefinitionStub(seeded, "rule_1");
    expect(again).toBe(seeded);
  });

  it("ensureConfidenceMatchRuleDefinitionStub preserves other definitions", () => {
    const { doc } = appendUniqueMatchDefinitionStub({});
    const withExtra = ensureConfidenceMatchRuleDefinitionStub(doc, "custom_rule");
    const map = confidenceMatchDefinitionsAsMap(withExtra);
    expect(Object.keys(map).sort()).toEqual(["custom_rule", "rule_1"]);
    expect(map.custom_rule?.name).toBe("custom_rule");
  });

  it("nextUniqueMatchDefinitionId skips occupied keys case-insensitively", () => {
    const doc = {
      validation_rule_definitions: {
        RULE_1: { name: "RULE_1" },
      },
    };
    expect(nextUniqueMatchDefinitionId(doc)).toBe("rule_2");
  });

  it("appendUniqueMatchDefinitionStub uses namePrefix for new ids", () => {
    const { newId } = appendUniqueMatchDefinitionStub({}, "prefix_suffix");
    expect(newId).toBe("prefix_suffix_1");
  });
});
