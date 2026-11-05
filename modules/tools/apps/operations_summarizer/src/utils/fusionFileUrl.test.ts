import { describe, expect, it, vi } from "vitest";
import {
  buildFusionFileSearchUrl,
  clusterHostFromBaseUrl,
  resolveFusionFileUrl,
  resolveFusionOrigin,
} from "./fusionFileUrl";

describe("buildFusionFileSearchUrl", () => {
  it("builds a classic Fusion file link from project, cluster, and numeric id", () => {
    const url = buildFusionFileSearchUrl({
      fusionOrigin: "https://example.fusion.cognite.com",
      project: "example-project",
      clusterHost: "example.cognitedata.com",
      fileId: 1241735767557533,
      searchQuery: "Summary period: 2026-10-21 to 2026-10-28",
    });

    expect(url).toBe(
      "https://example.fusion.cognite.com/example-project/search/file/1241735767557533?cluster=example.cognitedata.com&workspace=flows#searchCategory=%22file%22&searchQuery=%22Summary+period%3A+2026-10-21+to+2026-10-28%22&preview_instance=%7B%22id%22%3A1241735767557533%2C%22type%22%3A%22file%22%7D",
    );
  });
});

describe("clusterHostFromBaseUrl", () => {
  it("reads the host from the client base URL", () => {
    expect(clusterHostFromBaseUrl("https://example.cognitedata.com")).toBe(
      "example.cognitedata.com",
    );
  });
});

describe("resolveFusionOrigin", () => {
  it("uses the deployment org when the app is running locally", () => {
    expect(resolveFusionOrigin("https://localhost:3001")).toBe(
      "https://example.fusion.cognite.com",
    );
  });

  it("keeps the current Fusion host", () => {
    expect(resolveFusionOrigin("https://acme.fusion.cognite.com")).toBe(
      "https://acme.fusion.cognite.com",
    );
  });
});

describe("resolveFusionFileUrl", () => {
  it("looks up the numeric file id and uses the client project and cluster", async () => {
    const retrieve = vi.fn().mockResolvedValue([{ id: 1241735767557533 }]);
    const url = await resolveFusionFileUrl(
      {
        project: "example-project",
        getBaseUrl: () => "https://example.cognitedata.com",
        files: { retrieve },
      },
      {
        space: "ops_summaries",
        externalId: "ops_summary_1",
        searchQuery: "Summary period: 2026-10-21 to 2026-10-28",
      },
    );

    expect(retrieve).toHaveBeenCalledWith([
      {
        instanceId: { space: "ops_summaries", externalId: "ops_summary_1" },
      },
    ]);
    expect(url).toContain(
      "https://example.fusion.cognite.com/example-project/search/file/1241735767557533",
    );
    expect(url).toContain("cluster=example.cognitedata.com");
    expect(url).toContain("workspace=flows");
  });
});
