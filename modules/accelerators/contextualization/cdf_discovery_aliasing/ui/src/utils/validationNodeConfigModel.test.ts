import { describe, expect, it } from "vitest";
import {
  parseValidationNodeConfig,
  serializeValidationNodeConfig,
} from "./validationNodeConfigModel";

describe("validationNodeConfigModel", () => {
  it("round-trips validation_rule_definitions", () => {
    const cfg = {
      description: "Asset blacklist",
      validation_rule_definitions: {
        blacklist: {
          name: "blacklist",
          enabled: true,
          priority: 10,
          match: { keywords: ["test"], expressions: [] },
          confidence_modifier: { mode: "explicit", value: 0 },
        },
      },
    };
    const parsed = parseValidationNodeConfig(cfg);
    expect(parsed.definitionEntries).toHaveLength(1);
    expect(parsed.definitionEntries[0]?.id).toBe("blacklist");
    const out = serializeValidationNodeConfig({
      ...parsed,
      description: parsed.description,
    });
    expect(out.description).toBe("Asset blacklist");
    const defs = out.validation_rule_definitions as Record<string, unknown>;
    expect(defs.blacklist).toBeTruthy();
  });
});
