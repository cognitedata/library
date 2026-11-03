/**
 * CDF Summary Hooks.
 * Server-side prefix filters + cursor pagination for lists; bounded picker for tests.
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { getSummarySpace } from "@/config/dataTypes";
import { useCdfClient } from "@/contexts/CdfClientContext";
import { summaryInstanceMatchesContentTypes } from "@/hooks/useSummaryFilters";
import type { DataTypeName, SummaryInstance } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { buildFilesUploadLinkPath } from "@/utils/api";
import { sortByCreatedTimeDesc } from "@/utils/array";
import { withCdfConcurrency } from "@/utils/cdfRequest";
import { toCalendarIsoDate } from "@/utils/dates";
import { formatDateRangeDisplay } from "@/utils/formatting";
import {
  dedupeSummaryInstancesByNodeId,
  firstSummaryInstanceOrNull,
  mapNodesToSummaryInstances,
} from "@/utils/summaryInstanceMapping";
import { collectFilteredSummaryPage } from "@/utils/summaryListClientFilter";
import {
  buildLatestSummaryListRequest,
  buildSummaryInstancesQueryRequest,
  buildSummaryListRequest,
  buildSummaryServerFilterFingerprint,
  fetchOperationSummariesForPicker,
  formatSummaryContentTypeBitmapLine,
  getOperationSummaryExternalIdPrefix,
  getOperationSummaryPickerMax,
  getSummaryListPageSize,
  parseSummaryQueryPageResponse,
  type SummaryListVariant,
  type SummaryServerListFilters,
} from "@/utils/summaryListQuery";
import { computeTestReportStatsStreaming } from "@/utils/testReportStatsStreaming";
import { CDF_QUERY_KEYS } from "./cdfQueryKeys";

type UseSummariesCdfDeps = {
  useCdfClient: typeof useCdfClient;
  withCdfConcurrency: typeof withCdfConcurrency;
};

const defaultSummariesCdfDeps: UseSummariesCdfDeps = {
  useCdfClient,
  withCdfConcurrency,
};

export const UseSummariesCdfContext = createContext<UseSummariesCdfDeps>(
  defaultSummariesCdfDeps,
);

function useSummariesCdfDeps(): UseSummariesCdfDeps {
  return useContext(UseSummariesCdfContext);
}

function queryErrorToError(err: unknown): Error | null {
  if (err == null) return null;
  if (err instanceof Error) return err;
  return new Error(String(err));
}

// ============================================================
// Paged summary list (single DMS page per view)
// ============================================================

export type { SummaryServerListFilters };

export interface UseSummaryListPageOptions {
  variant: SummaryListVariant;
  pageSize?: number;
  serverFilters: SummaryServerListFilters;
}

export interface UseSummaryListPageResult {
  items: SummaryInstance[];
  nextCursor: string | undefined;
  isLoading: boolean;
  isFetching: boolean;
  isError: boolean;
  error: Error | null;
  refetch: () => Promise<unknown>;
  pageIndex: number;
  pageDisplay: number;
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  goToNextPage: () => void;
  goToPreviousPage: () => void;
  resetPagination: () => void;
}

/**
 * Cursor-based server pagination for operation or test summary tables.
 */
export function useSummaryListPage(
  options: UseSummaryListPageOptions,
): UseSummaryListPageResult {
  const {
    variant,
    pageSize = getSummaryListPageSize(),
    serverFilters,
  } = options;
  const { useCdfClient: useClient, withCdfConcurrency } = useSummariesCdfDeps();
  const { sdk, isAuthLoading } = useClient();

  const filterFingerprint = useMemo(
    () => buildSummaryServerFilterFingerprint(serverFilters),
    [serverFilters],
  );

  const [pagination, setPagination] = useState<{
    pageIndex: number;
    cursors: (string | undefined)[];
  }>({ pageIndex: 0, cursors: [undefined] });

  /** Matches that belong on the next UI page after client-side content-type filtering. */
  const filteredOverflowRef = useRef<SummaryInstance[]>([]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: reset cursors when filters, variant, or page size change (effect body intentionally constant).
  useEffect(() => {
    filteredOverflowRef.current = [];
    setPagination({ pageIndex: 0, cursors: [undefined] });
  }, [filterFingerprint, variant, pageSize]);

  const cursorForPage = pagination.cursors[pagination.pageIndex] ?? undefined;
  const cursorKey = cursorForPage ?? "start";

  const query = useQuery({
    queryKey: CDF_QUERY_KEYS.summariesPage(
      variant,
      filterFingerprint,
      pageSize,
      cursorKey,
    ),
    queryFn: async () => {
      if (!sdk) throw new Error("SDK not available");

      const fetchServerPage = async (
        cursor: string | undefined,
      ): Promise<{
        items: SummaryInstance[];
        nextCursor: string | undefined;
        usedListApi: boolean;
        rawCount: number;
      }> => {
        if (variant === "operation") {
          const req = buildSummaryInstancesQueryRequest({
            limit: pageSize,
            cursor,
            filters: serverFilters,
          });
          const response = await withCdfConcurrency(() =>
            sdk.instances.query(req),
          );
          const { items, nextCursor } = parseSummaryQueryPageResponse(response);
          return {
            items: mapNodesToSummaryInstances(items),
            nextCursor,
            usedListApi: false,
            rawCount: items.length,
          };
        }
        const req = buildSummaryListRequest({
          variant,
          limit: pageSize,
          cursor,
          filters: serverFilters,
        });
        const response = await withCdfConcurrency(() =>
          sdk.instances.list(req),
        );
        return {
          items: mapNodesToSummaryInstances(response.items),
          nextCursor: response.nextCursor,
          usedListApi: true,
          rawCount: response.items.length,
        };
      };

      // Content type is free-form metadata (`metadata.contentTypes`) and an OPS: bitmap
      // in CogniteFile description. The view has no filterable content-type property,
      // and legacy rows match only via description heuristics, so this stays a bounded client scan.
      const contentTypeFilterActive =
        serverFilters.contentTypeFilterEnabled &&
        serverFilters.selectedContentTypes.length > 0;

      if (!contentTypeFilterActive) {
        let { items, nextCursor, usedListApi, rawCount } =
          await fetchServerPage(cursorForPage);
        // instances.list returns a cursor even on the final page. Discard it when
        // fewer raw SDK items than the limit were returned — there is nothing left to fetch.
        if (usedListApi && rawCount < pageSize) nextCursor = undefined;
        return {
          items: dedupeSummaryInstancesByNodeId(items),
          nextCursor,
        };
      }

      const overflow = filteredOverflowRef.current;
      filteredOverflowRef.current = [];

      const collected = await collectFilteredSummaryPage({
        pageSize,
        serverCursor: cursorForPage,
        overflow,
        fetchPage: fetchServerPage,
        matches: (summary) =>
          summaryInstanceMatchesContentTypes(
            summary,
            serverFilters.selectedContentTypes,
          ),
      });

      filteredOverflowRef.current = collected.overflow;

      return {
        items: dedupeSummaryInstancesByNodeId(collected.items),
        nextCursor: collected.nextCursor,
      };
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 2 * 60 * 1000,
  });

  const goToNextPage = useCallback(() => {
    const nc = query.data?.nextCursor;
    if (!nc) return;
    setPagination((prev) => ({
      pageIndex: prev.pageIndex + 1,
      cursors: [...prev.cursors.slice(0, prev.pageIndex + 1), nc],
    }));
  }, [query.data?.nextCursor]);

  const goToPreviousPage = useCallback(() => {
    setPagination((prev) => ({
      ...prev,
      pageIndex: Math.max(0, prev.pageIndex - 1),
    }));
  }, []);

  const resetPagination = useCallback(() => {
    setPagination({ pageIndex: 0, cursors: [undefined] });
  }, []);

  const items = query.data?.items ?? [];
  const nextCursor = query.data?.nextCursor;

  return {
    items,
    nextCursor,
    isLoading: query.isLoading,
    isFetching: query.isFetching,
    isError: query.isError,
    error: queryErrorToError(query.error),
    refetch: query.refetch,
    pageIndex: pagination.pageIndex,
    pageDisplay: pagination.pageIndex + 1,
    hasNextPage: !!nextCursor,
    hasPreviousPage: pagination.pageIndex > 0,
    goToNextPage,
    goToPreviousPage,
    resetPagination,
  };
}

// ============================================================
// Latest instance (dashboard)
// ============================================================

export function useLatestSummaryInstance(variant: SummaryListVariant) {
  const { useCdfClient: useClient, withCdfConcurrency } = useSummariesCdfDeps();
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.summariesLatest(variant),
    queryFn: async () => {
      if (!sdk) throw new Error("SDK not available");
      if (variant === "operation") {
        const qreq = buildSummaryInstancesQueryRequest({
          limit: 1,
          filters: {
            dateFilterEnabled: false,
            dateRange: null,
            contentTypeFilterEnabled: false,
            selectedContentTypes: [],
          },
        });
        const qres = await withCdfConcurrency(() => sdk.instances.query(qreq));
        const { items } = parseSummaryQueryPageResponse(qres);
        return firstSummaryInstanceOrNull(items);
      }
      const req = buildLatestSummaryListRequest({ variant });
      const response = await withCdfConcurrency(() => sdk.instances.list(req));
      return firstSummaryInstanceOrNull(response.items);
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 2 * 60 * 1000,
  });
}

// ============================================================
// Operation summaries picker (bounded)
// ============================================================

export function useOperationSummariesPicker() {
  const { useCdfClient: useClient } = useSummariesCdfDeps();
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.summariesPicker(),
    queryFn: async () => {
      if (!sdk) throw new Error("SDK not available");
      const { items } = await fetchOperationSummariesForPicker(
        sdk,
        getOperationSummaryPickerMax(),
      );
      return dedupeSummaryInstancesByNodeId(
        sortByCreatedTimeDesc(mapNodesToSummaryInstances(items)),
      );
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 2 * 60 * 1000,
  });
}

// ============================================================
// Streaming test report stats (dashboard / history headers)
// ============================================================

export function useTestReportStatsStreaming() {
  const { useCdfClient: useClient } = useSummariesCdfDeps();
  const { sdk, isAuthLoading } = useClient();

  return useQuery({
    queryKey: CDF_QUERY_KEYS.testReportStatsStreaming(),
    queryFn: async () => {
      if (!sdk) throw new Error("SDK not available");
      return computeTestReportStatsStreaming(sdk);
    },
    enabled: !!sdk && !isAuthLoading,
    staleTime: 2 * 60 * 1000,
  });
}

// ============================================================
// Mutations
// ============================================================

/**
 * Hook to delete a summary
 */
export function useDeleteSummary() {
  const { useCdfClient: useClient, withCdfConcurrency } = useSummariesCdfDeps();
  const { sdk } = useClient();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (summary: SummaryInstance) => {
      if (!sdk) throw new Error("SDK not available");

      await withCdfConcurrency(
        () =>
          sdk.instances.delete([
            {
              instanceType: "node",
              space: summary.space || getSummarySpace(),
              externalId: summary.externalId,
            },
          ]),
        { delete: true },
      );

      return true;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: CDF_QUERY_KEYS.summaries });
    },
  });
}

/**
 * Hook to save a summary to CDF as PDF
 */
export function useSaveSummary() {
  const { useCdfClient: useClient, withCdfConcurrency } = useSummariesCdfDeps();
  const { sdk } = useClient();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({
      content,
      startDate,
      endDate,
      contentTypes,
    }: {
      content: string;
      startDate: Date;
      endDate: Date;
      contentTypes: string[];
    }) => {
      if (!sdk) throw new Error("SDK not available");

      const pdfModule = await import("../utils/pdfGenerator").catch(
        (error: unknown) => {
          console.error("Failed to load PDF generator:", error);
          throw new Error("PDF_MODULE_LOAD_FAILED");
        },
      );
      const { generatePdfFromMarkdown } = pdfModule;

      const viewId = VIEW_IDS.SUMMARY_OUTPUT_FILE;
      const now = new Date();
      const startStr = toCalendarIsoDate(startDate);
      const endStr = toCalendarIsoDate(endDate);
      const nowDateStr = toCalendarIsoDate(now);
      const nowTimeStr = now.toTimeString().split(" ")[0].replace(/:/g, "_");
      const externalId = `${getOperationSummaryExternalIdPrefix()}${startStr}_${endStr}_${nowDateStr}_${nowTimeStr}_${now.getTime()}`;
      const humanReadableName = `Operations summary, ${formatDateRangeDisplay(startDate, endDate)}`;
      const fileName = `${humanReadableName}.pdf`;

      const pdfBlob = generatePdfFromMarkdown(content, {
        title: "Operation Summary",
      });

      await withCdfConcurrency(() =>
        sdk.instances.upsert({
          items: [
            {
              instanceType: "node",
              space: getSummarySpace(),
              externalId,
              sources: [
                {
                  source: {
                    type: "view",
                    space: viewId.space,
                    externalId: viewId.externalId,
                    version: viewId.version,
                  },
                  properties: {
                    name: fileName,
                    description: `${formatSummaryContentTypeBitmapLine(contentTypes as DataTypeName[])}${humanReadableName}. Content types: ${contentTypes.join(", ")}`,
                    mimeType: "application/pdf",
                    sourceContext: content,
                  },
                },
              ],
            },
          ],
        }),
      );

      const uploadLinkResponse = await withCdfConcurrency(() =>
        sdk.post<{
          items: Array<{
            instanceId: { space: string; externalId: string };
            uploadUrl: string;
          }>;
        }>(buildFilesUploadLinkPath(sdk.project), {
          data: {
            items: [{ instanceId: { space: getSummarySpace(), externalId } }],
          },
        }),
      );

      if (!uploadLinkResponse.data?.items?.[0]?.uploadUrl) {
        throw new Error("Failed to get upload URL");
      }

      const uploadUrl = uploadLinkResponse.data.items[0].uploadUrl;

      // The upload URL is returned by the CDF SDK; binary content goes directly to S3 per
      // the CDF Files API upload flow — this is not a raw CDF call.
      const uploadResponse = await fetch(uploadUrl, {
        method: "PUT",
        headers: { "Content-Type": "application/pdf" },
        body: pdfBlob,
      });

      if (!uploadResponse.ok) {
        throw new Error(
          `Failed to upload PDF: ${uploadResponse.status} ${uploadResponse.statusText}`,
        );
      }

      return { externalId, fileName };
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: CDF_QUERY_KEYS.summaries });
    },
  });
}
