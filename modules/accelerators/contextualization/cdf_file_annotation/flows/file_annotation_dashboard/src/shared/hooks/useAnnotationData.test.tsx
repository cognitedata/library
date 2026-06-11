import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { type ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { CogniteClient } from "@cognite/sdk";
import type { GroupedCoverage, PipelineConfig } from "@/shared/utils/types";
import { DataProcessor } from "@/shared/utils/dataProcessor";
import {
  useAnnotationOverviewMetrics,
  useAnnotationSummary,
} from "./useAnnotationData";

vi.mock("@/runtime/authMode", () => ({
  isLocalMockMode: false,
}));

type RawRow = {
  key: string;
  columns: Record<string, unknown>;
};

const asAsyncIterable = (rows: RawRow[]) => ({
  async *[Symbol.asyncIterator]() {
    for (const row of rows) {
      yield row;
    }
  },
});

const normalizeGroupedCoverage = (rows: GroupedCoverage[]) =>
  [...rows].sort((a, b) => a.groupKey.localeCompare(b.groupKey));

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });

  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe("useAnnotationOverviewMetrics", () => {
  it("matches summary-based overview metrics", async () => {
    const rawByTable: Record<string, RawRow[]> = {
      asset_tags: [
        {
          key: "file1:asset:1",
          columns: {
            startNode: "files:file1",
            endNode: "asset-a",
            endNodeResourceType: "Pump",
            status: "Approved",
          },
        },
        {
          key: "file2:asset:1",
          columns: {
            startNode: "files:file2",
            endNode: "asset-b",
            endNodeResourceType: "Valve",
            status: "Approved",
          },
        },
      ],
      file_tags: [
        {
          key: "file1:file:1",
          columns: {
            startNode: "files:file1",
            endNode: "file-a",
            endNodeResourceType: "Document",
            status: "Approved",
          },
        },
      ],
      pattern_tags: [
        {
          key: "pattern_file1:1",
          columns: {
            startNode: "files:file1",
            endNode: "asset-c",
            endNodeResourceType: "Pump",
            status: "Suggested",
          },
        },
        {
          key: "pattern_file2:2",
          columns: {
            startNode: "files:file2",
            endNode: "asset-d",
            endNodeResourceType: "Valve",
            status: "Approved",
          },
        },
        {
          key: "pattern_file3:3",
          columns: {
            startNode: "files:file3",
            endNode: "asset-e",
            endNodeResourceType: "Motor",
            status: "Suggested",
          },
        },
        {
          key: "pattern_file3:ignored-empty-end-node",
          columns: {
            startNode: "files:file3",
            endNode: "",
            endNodeResourceType: "Motor",
            status: "Suggested",
          },
        },
      ],
    };

    const fileMetadataById: Record<string, Record<string, unknown>> = {
      file1: {
        name: "file1.pdf",
        sourceId: "src-file1",
        resourceType: "Drawing",
        secondaryScope: "Area-A",
      },
      file2: {
        name: "file2.pdf",
        sourceId: "src-file2",
        resourceType: "Specification",
        secondaryScope: "Area-B",
      },
      file3: {
        name: "file3.pdf",
        sourceId: "src-file3",
        resourceType: "Drawing",
        secondaryScope: "Area-A",
      },
    };

    const sdk = {
      raw: {
        listRows: vi.fn((_db: string, tableName: string) => {
          return asAsyncIterable(rawByTable[tableName] ?? []);
        }),
      },
      instances: {
        retrieve: vi.fn(async (request: { items: Array<{ externalId: string }> }) => ({
          items: request.items.map((item) => ({
            externalId: item.externalId,
            properties: {
              file_space: {
                "file_view/1": fileMetadataById[item.externalId] ?? {},
              },
            },
          })),
        })),
      },
    } as unknown as CogniteClient;

    const config: PipelineConfig = {
      rawDb: "db",
      rawTableAssetTags: "asset_tags",
      rawTableFileTags: "file_tags",
      rawTablePatternTags: "pattern_tags",
      fileView: {
        schemaSpace: "file_space",
        externalId: "file_view",
        version: "1",
        instanceSpace: "files",
      },
      fileResourceProperty: "resourceType",
      secondaryScopeProperty: "secondaryScope",
    };

    const wrapper = createWrapper();

    const summary = renderHook(
      () => useAnnotationSummary(sdk, config, "pipe-1", { enabled: true }),
      { wrapper }
    );

    await waitFor(() => {
      expect(summary.result.current.isSuccess).toBe(true);
    });

    const summaryData = summary.result.current.data;
    expect(summaryData).toBeDefined();

    const expectedOverall = DataProcessor.calculateCoverage(
      summaryData?.actual ?? [],
      summaryData?.potential ?? []
    );
    const expectedByTagResource = DataProcessor.calculateGroupedCoverage(
      summaryData?.actual ?? [],
      summaryData?.potential ?? [],
      "endNodeResourceType"
    );
    const expectedByFileResource = DataProcessor.calculateGroupedCoverage(
      summaryData?.actual ?? [],
      summaryData?.potential ?? [],
      "fileResourceType"
    );
    const expectedBySecondaryScope = DataProcessor.calculateGroupedCoverage(
      summaryData?.actual ?? [],
      summaryData?.potential ?? [],
      "fileSecondaryScope"
    );

    const overview = renderHook(
      () => useAnnotationOverviewMetrics(sdk, config, "pipe-1", { enabled: true }),
      { wrapper }
    );

    await waitFor(() => {
      expect(overview.result.current.isSuccess).toBe(true);
    });

    const overviewData = overview.result.current.data;
    expect(overviewData).toBeDefined();

    expect(overviewData?.overallCoverage).toEqual(expectedOverall);
    expect(normalizeGroupedCoverage(overviewData?.coverageByTagResourceType ?? [])).toEqual(
      normalizeGroupedCoverage(expectedByTagResource)
    );
    expect(normalizeGroupedCoverage(overviewData?.coverageByFileResourceType ?? [])).toEqual(
      normalizeGroupedCoverage(expectedByFileResource)
    );
    expect(normalizeGroupedCoverage(overviewData?.coverageBySecondaryScope ?? [])).toEqual(
      normalizeGroupedCoverage(expectedBySecondaryScope)
    );
  });
});
