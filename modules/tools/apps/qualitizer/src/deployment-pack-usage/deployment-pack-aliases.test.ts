import { describe, expect, it } from "vitest";
import {
  deploymentPackDisplayName,
  normalizeDeploymentPackId,
  normalizePackInUse,
  packIdsFromNormalizedInUse,
} from "./deployment-pack-aliases";

describe("normalizeDeploymentPackId", () => {
  it("maps legacy ids to canonical ids", () => {
    expect(normalizeDeploymentPackId("dp:tool:qualitizer")).toBe("dp:app:qualitizer");
    expect(normalizeDeploymentPackId("dp:infield:cdf_infield_location")).toBe(
      "dp:acc:infield_quickstart"
    );
    expect(normalizeDeploymentPackId("dp:dashboards:rpt_quality")).toBe("dp:dashboards:report_quality");
    expect(normalizeDeploymentPackId("dp:models:rmdm_v1")).toBe("dp:models:rmdm");
    expect(normalizeDeploymentPackId("dp:sourcesystem:cdf_pi")).toBe("cdf_pi");
    expect(normalizeDeploymentPackId("dp:contextualization:cdf_file_annotation")).toBe(
      "dp:acc:ctx:cdf_file_annotation"
    );
    expect(normalizeDeploymentPackId("dp:models:cfihos_oil_and_gas_extension_search")).toBe(
      "dp:models:cfihos_oil_and_gas_extension"
    );
    expect(normalizeDeploymentPackId("dp:models:cdf_process_industry_extension")).toBe(
      "dp:models:cdf_process_industry_extension"
    );
    expect(normalizeDeploymentPackId("dp:atlas_ai_extractor")).toBe("dp:atlas:ai_property_extractor");
    expect(normalizeDeploymentPackId("dp:cdf_common")).toBe("dp:accelerators:cdf_common");
    expect(normalizeDeploymentPackId("dp:cdf_entity_matching")).toBe("dp:acc:ctx:cdf_entity_matching");
    expect(normalizeDeploymentPackId("dp:cdf_file_annotation")).toBe("dp:acc:ctx:cdf_file_annotation");
    expect(normalizeDeploymentPackId("dp:cdf_p_and_id_annotation")).toBe("dp:acc:ctx:cdf_p_and_id_annotation");
    expect(normalizeDeploymentPackId("dp:cdf_p_and_id_parser")).toBe("dp:acc:ctx:p_and_id_parser");
    expect(normalizeDeploymentPackId("dp:context_quality")).toBe("dp:dashboards:context_quality");
    expect(normalizeDeploymentPackId("dp:contextualization:cdf_entity_matching")).toBe(
      "dp:acc:ctx:cdf_entity_matching"
    );
    expect(normalizeDeploymentPackId("dp:contextualization:cdf_p_and_id_annotation")).toBe(
      "dp:acc:ctx:cdf_p_and_id_annotation"
    );
    expect(normalizeDeploymentPackId("dp:open_industrial_data_sync")).toBe("dp:acc:cdf_oid_sync");
    expect(normalizeDeploymentPackId("dp:project_health")).toBe("dp:dashboards:project_health");
  });

  it("maps friendly names to canonical ids", () => {
    expect(normalizeDeploymentPackId("Qualitizer")).toBe("dp:app:qualitizer");
    expect(normalizeDeploymentPackId("InField QuickStart")).toBe("dp:acc:infield_quickstart");
    expect(normalizeDeploymentPackId("CDF Common")).toBe("dp:accelerators:cdf_common");
  });

  it("matches synonyms and ids case-insensitively", () => {
    expect(normalizeDeploymentPackId("DP:cdf_common")).toBe("dp:accelerators:cdf_common");
    expect(normalizeDeploymentPackId("DP:Accelerators:CDF_Common")).toBe("dp:accelerators:cdf_common");
    expect(normalizeDeploymentPackId("DP:TOOL:Qualitizer")).toBe("dp:app:qualitizer");
  });

  it("leaves unknown values unchanged", () => {
    expect(normalizeDeploymentPackId("dp:unknown:pack")).toBe("dp:unknown:pack");
  });
});

describe("normalizePackInUse", () => {
  it("merges synonyms into one canonical key", () => {
    expect(
      normalizePackInUse({
        Qualitizer: true,
        "dp:tool:qualitizer": true,
        "dp:app:qualitizer": true,
      })
    ).toEqual({ "dp:app:qualitizer": true });
  });

  it("deduplicates pack id list after normalization", () => {
    expect(
      packIdsFromNormalizedInUse({
        "InField QuickStart": true,
        "dp:infield:cdf_infield_location": true,
      })
    ).toEqual(["dp:acc:infield_quickstart"]);
  });
});

describe("deploymentPackDisplayName", () => {
  it("returns friendly label for canonical and legacy ids", () => {
    expect(deploymentPackDisplayName("dp:app:qualitizer")).toBe("Qualitizer");
    expect(deploymentPackDisplayName("dp:tool:qualitizer")).toBe("Qualitizer");
    expect(deploymentPackDisplayName("dp:dashboards:rpt_quality")).toBe(
      "Quality Reports — Contextualization Rate"
    );
  });
});
