import { describe, expect, it } from "vitest";
import { connectEndMenuOptionsForSourceType } from "./connectEndMenuOptions";

describe("connectEndMenuOptionsForSourceType", () => {
  it("returns no connect-end palette targets for persistence nodes (out may wire only to End)", () => {
    expect(connectEndMenuOptionsForSourceType("keaViewSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("keaRawSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("keaClassicSave")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("keaAliasPersistence")).toEqual([]);
    expect(connectEndMenuOptionsForSourceType("keaInvertedIndex")).toEqual([]);
  });

  it("still offers transform from view query", () => {
    const opts = connectEndMenuOptionsForSourceType("keaViewQuery");
    expect(opts.some((o) => o.payload.kind === "discovery" && o.payload.stage === "transform")).toBe(true);
  });
});
