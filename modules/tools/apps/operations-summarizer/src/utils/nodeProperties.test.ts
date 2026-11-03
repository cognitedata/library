import { describe, expect, it } from "vitest";
import type { ViewId } from "@/types/operations";
import { extractFileProperties, extractNodeProperties } from "./nodeProperties";

const viewId: ViewId = { space: "s", externalId: "v", version: "1" };
const viewKey = "s/v/1";

describe("extractNodeProperties", () => {
  it("returns properties for direct view key match", () => {
    const node = {
      properties: {
        [viewKey]: { name: "test", value: 42 },
      },
    } as { properties: Record<string, Record<string, string | number>> };
    expect(extractNodeProperties(node, viewId)).toEqual({
      name: "test",
      value: 42,
    });
  });

  it("falls back to first non-empty view properties", () => {
    const node = {
      properties: {
        other: { foo: "bar" },
      },
    } as { properties: Record<string, Record<string, string | number>> };
    expect(extractNodeProperties(node, viewId)).toEqual({ foo: "bar" });
  });

  it("returns empty object when no properties", () => {
    expect(extractNodeProperties({}, viewId)).toEqual({});
    expect(extractNodeProperties({ properties: {} }, viewId)).toEqual({});
  });
});

describe("extractFileProperties", () => {
  it("returns properties for flat key", () => {
    const node = {
      properties: {
        [viewKey]: { name: "file.txt", content: "body" },
      },
    };
    expect(extractFileProperties(node, viewId)).toEqual({
      name: "file.txt",
      content: "body",
    });
  });

  it("returns undefined when node has no properties", () => {
    expect(extractFileProperties({}, viewId)).toBeUndefined();
  });

  it("tries nested structure space[externalId/version]", () => {
    const node = {
      properties: {
        s: { "v/1": { title: "nested" } },
      },
    };
    expect(extractFileProperties(node, viewId)).toEqual({ title: "nested" });
  });
});
