import { describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import type { SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { resolveSummaryContent } from "./testingContent";

const viewKey = `${VIEW_IDS.SUMMARY_OUTPUT_FILE.space}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.externalId}/${VIEW_IDS.SUMMARY_OUTPUT_FILE.version}`;

describe("resolveSummaryContent", () => {
  it("returns content from CogniteFile view properties", async () => {
    const summary: SummaryInstance = {
      externalId: "sum-1",
      space: "ops_summaries",
      createdTime: 1,
      properties: {
        [viewKey]: {
          content: "# From props\n\nBody",
        },
      },
    };

    await expect(
      resolveSummaryContent(summary, null, "ops_summaries"),
    ).resolves.toContain("From props");
  });

  it("falls back to nested sourceContext in properties", async () => {
    const summary: SummaryInstance = {
      externalId: "sum-2",
      space: "ops_summaries",
      createdTime: 1,
      properties: {
        nested: {
          v1: {
            sourceContext: "# Markdown\nhere",
          },
        },
      } as SummaryInstance["properties"],
    };

    await expect(
      resolveSummaryContent(summary, null, "ops_summaries"),
    ).resolves.toContain("Markdown");
  });

  it("fetches from CDF when properties empty and sdk provided", async () => {
    const summary: SummaryInstance = {
      externalId: "file-id",
      space: "ops_summaries",
      createdTime: 1,
      properties: {},
    };

    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/f" }]);

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: { get: () => "text/markdown" },
      text: async () => "downloaded md",
    });

    const content = await resolveSummaryContent(
      summary,
      cogniteClientMock({
        project: "p",
        files: { getDownloadUrls },
      }),
      "ops_summaries",
    );

    expect(content).toBe("downloaded md");
    expect(getDownloadUrls).toHaveBeenCalledWith([
      { instanceId: { space: "ops_summaries", externalId: "file-id" } },
    ]);
  });

  it("returns empty string when nothing found", async () => {
    const summary: SummaryInstance = {
      externalId: "x",
      space: "s",
      createdTime: 1,
      properties: {},
    };

    await expect(resolveSummaryContent(summary, null, "s")).resolves.toBe("");
  });

  it("uses instances.retrieve when slim properties omit sourceContext", async () => {
    const summary: SummaryInstance = {
      externalId: "sum-ret",
      space: "ops_summaries",
      createdTime: 1,
      properties: {
        [viewKey]: {
          name: "n.pdf",
          description: "OPS:11111\nHuman line only",
          mimeType: "application/pdf",
        },
      },
    };

    const retrieve = vi.fn().mockResolvedValue({
      items: [
        {
          instanceType: "node",
          externalId: "sum-ret",
          space: "ops_summaries",
          createdTime: 1,
          properties: {
            [viewKey]: {
              name: "n.pdf",
              sourceContext: "# Operation Summary\n\nBody from retrieve",
              mimeType: "application/pdf",
            },
          },
        },
      ],
    });

    const getDownloadUrls = vi.fn();

    await expect(
      resolveSummaryContent(
        summary,
        cogniteClientMock({
          project: "p",
          files: { getDownloadUrls },
          instances: { retrieve },
        }),
        "ops_summaries",
      ),
    ).resolves.toContain("Body from retrieve");

    expect(retrieve).toHaveBeenCalled();
    expect(getDownloadUrls).not.toHaveBeenCalled();
  });
});
