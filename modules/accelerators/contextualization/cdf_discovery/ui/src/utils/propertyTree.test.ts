/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import {
  childCount,
  childPropertyEntries,
  normalizePropertyPayload,
  propertyValueKind,
  sortPropertyKeys,
} from "./propertyTree";

describe("propertyTree", () => {
  it("orders preferred keys first", () => {
    expect(sortPropertyKeys(["z", "kind", "name", "id"], ["kind", "id", "label"])).toEqual([
      "kind",
      "id",
      "name",
      "z",
    ]);
  });

  it("classifies value kinds", () => {
    expect(propertyValueKind(null)).toBe("null");
    expect(propertyValueKind("x")).toBe("primitive");
    expect(propertyValueKind({ a: 1 })).toBe("object");
    expect(propertyValueKind([1])).toBe("array");
  });

  it("builds nested object and array children", () => {
    const entries = childPropertyEntries({ b: 2, a: 1 }, "root", ["b", "a"]);
    expect(entries.map((e) => e.key)).toEqual(["b", "a"]);
    expect(childPropertyEntries([10, 20], "items")[1]).toEqual({
      key: "[1]",
      path: "items[1]",
      value: 20,
    });
  });

  it("normalizes payloads with preferred key order", () => {
    const out = normalizePropertyPayload({ z: 1, kind: "folder", id: "x" });
    expect(Object.keys(out)).toEqual(["kind", "id", "z"]);
  });

  it("counts children", () => {
    expect(childCount({ a: 1, b: 2 })).toBe(2);
    expect(childCount([1, 2, 3])).toBe(3);
  });
});
