import { describe, expect, it } from "vitest";
import {
  buildLineageScript,
  buildLineageScriptsShell,
  buildLineageScriptEntries,
  buildSapAllDatResourceName,
  deriveSiteCodeFromInstanceSpace,
  isDeferredMigrationLocation,
  resolveLegacyInstanceSpace,
  resolveMigrationSiteCode,
} from "./migration-scripts";
import type { LegacyConfigLocation } from "./types";

function location(
  assetExternalId: string,
  spaces?: { appDataInstanceSpace?: string; sourceDataInstanceSpace?: string },
  rowId?: string
): LegacyConfigLocation {
  return {
    rowId: rowId ?? assetExternalId,
    externalId: assetExternalId,
    dataSetId: null,
    assetExternalId,
    appDataInstanceSpace: spaces?.appDataInstanceSpace ?? "",
    sourceDataInstanceSpace: spaces?.sourceDataInstanceSpace ?? "",
    templateAdmins: [],
    checklistAdmins: [],
    fileFilter: [],
    assetFilter: [],
    generalFilter: [],
    timeseriesFilter: [],
    raw: {},
  };
}

describe("migration-scripts", () => {
  it("builds SAP resource names from site codes", () => {
    expect(buildSapAllDatResourceName("clk")).toBe("SAP-CLK-ALL-DAT");
  });

  it("builds lineage commands with skip labels", () => {
    expect(buildLineageScript("SAP-CLK-ALL-DAT")).toBe(
      "python migration/hybrid-asset-preparation.py process-dataset --asset-space SAP-CLK-ALL-DAT --data-set-name SAP-CLK-ALL-DAT --continue-on-error --skip-labels=equipment-class-characteristics,bom-items,bom-header"
    );
  });

  it("resolves site code from instance spaces like Infield 2 Config", () => {
    expect(deriveSiteCodeFromInstanceSpace("SAP-EN-ALL-DAT")).toBe("EN");
    expect(deriveSiteCodeFromInstanceSpace("APMA-PN-ALL-DAT")).toBe("PN");
    expect(
      resolveMigrationSiteCode(
        location("unparseable", { appDataInstanceSpace: "SAP-EN-ALL-DAT" })
      )
    ).toBe("EN");
    expect(resolveLegacyInstanceSpace(location("EN-ROOT"))).toBe("APMA-ROOT-ALL-DAT");
  });

  it("defers CL/FM pilot locations", () => {
    expect(isDeferredMigrationLocation(location("CL-CLK-PROD"))).toBe(true);
    expect(isDeferredMigrationLocation(location("FM-FRA-PROD"))).toBe(true);
    expect(
      isDeferredMigrationLocation(
        location("BI-ROOT", { appDataInstanceSpace: "SAP-CLK-ALL-DAT" })
      )
    ).toBe(true);
    expect(
      isDeferredMigrationLocation(
        location("BI-ROOT", { appDataInstanceSpace: "SAP-BI-ALL-DAT" })
      )
    ).toBe(false);
  });

  it("lists every wave 2 config row, not only resolvable site codes", () => {
    const locations = [
      location("CL-CLK-PROD"),
      location("BI-ROOT", { appDataInstanceSpace: "SAP-BI-ALL-DAT" }, "bi-row"),
      location("EN-ROOT", { appDataInstanceSpace: "SAP-EN-ALL-DAT" }, "en-row"),
      location("NP-UNPARSEABLE", {}, "np-row"),
    ];
    const entries = buildLineageScriptEntries(locations, {
      wave: "Wave 2",
      validation: {
        spaces: {
          "SAP-BI-ALL-DAT": {
            resourceName: "SAP-BI-ALL-DAT",
            exists: true,
            statusDetail: 'DMS space "SAP-BI-ALL-DAT" exists.',
            apiCalls: [],
          },
          "SAP-EN-ALL-DAT": {
            resourceName: "SAP-EN-ALL-DAT",
            exists: false,
            statusDetail: 'DMS space "SAP-EN-ALL-DAT" was not found.',
            apiCalls: [],
          },
          "SAP-ROOT-ALL-DAT": {
            resourceName: "SAP-ROOT-ALL-DAT",
            exists: false,
            statusDetail: 'DMS space "SAP-ROOT-ALL-DAT" was not found.',
            apiCalls: [],
          },
        },
        dataSets: {
          "SAP-BI-ALL-DAT": {
            resourceName: "SAP-BI-ALL-DAT",
            exists: true,
            statusDetail: 'Data set "SAP-BI-ALL-DAT" exists.',
            apiCalls: [],
          },
          "SAP-EN-ALL-DAT": {
            resourceName: "SAP-EN-ALL-DAT",
            exists: false,
            statusDetail: 'Data set "SAP-EN-ALL-DAT" was not found among 0 data sets in the project.',
            apiCalls: [],
          },
          "SAP-ROOT-ALL-DAT": {
            resourceName: "SAP-ROOT-ALL-DAT",
            exists: false,
            statusDetail: 'Data set "SAP-ROOT-ALL-DAT" was not found among 0 data sets in the project.',
            apiCalls: [],
          },
        },
      },
    });

    expect(entries.map((entry) => entry.rowId)).toEqual(["bi-row", "en-row", "np-row"]);
    expect(entries.find((entry) => entry.rowId === "bi-row")?.ready).toBe(true);
    expect(entries.find((entry) => entry.rowId === "en-row")?.ready).toBe(false);
    expect(entries.find((entry) => entry.rowId === "np-row")?.siteCode).toBeNull();
    expect(entries.find((entry) => entry.rowId === "np-row")?.resourceName).toBeNull();
  });

  it("joins ready scripts for shell copy", () => {
    const text = buildLineageScriptsShell(["SAP-CLK-ALL-DAT", "SAP-BI-ALL-DAT"]);
    expect(text.split("\n\n")).toHaveLength(2);
    expect(text).toContain("SAP-CLK-ALL-DAT");
    expect(text).toContain("SAP-BI-ALL-DAT");
  });
});
