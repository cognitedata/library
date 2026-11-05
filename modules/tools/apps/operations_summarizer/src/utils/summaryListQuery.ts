/**
 * Shared DMS reads for saved summary file nodes (CogniteFile in getSummarySpace()).
 * Operation-summary and test-report tables use `instances.query` with a slim CogniteFile
 * property projection (`instances.list` OpenAPI has no per-source property list).
 * The one-time test-report stats backfill still uses full `instances.list` (needs sourceContext).
 */

import type {
  CogniteClient,
  FilterDefinition,
  NodeOrEdgeListRequestV3,
  QueryRequest,
  QueryResponse,
  TableExpressionFilterDefinition,
} from "@cognite/sdk";
import { getAppConfig } from "@/config/appConfig";
import { getInstanceQueryLimit, getSummarySpace } from "@/config/dataTypes";
import type { DataTypeName } from "@/types/operations";
import { VIEW_IDS } from "@/types/operations";
import { withCdfConcurrency } from "@/utils/cdfRequest";

/** `instances.list` request shape for summary file nodes (typed for Cognite JS SDK). */
export type SummaryInstanceListRequest = NodeOrEdgeListRequestV3;

export function getOperationSummaryExternalIdPrefix(): string {
  return getAppConfig().externalIdPrefixes.operationSummary;
}

export function getTestReportExternalIdPrefix(): string {
  return getAppConfig().externalIdPrefixes.testReport;
}

/** Table UI page size (single DMS list call per view). */
export function getSummaryListPageSize(): number {
  return getAppConfig().limits.summaryListPageSize;
}

/** Max operation summaries loaded for test picker / dropdown (bounded memory). */
export function getOperationSummaryPickerMax(): number {
  return getAppConfig().limits.operationSummaryPickerMax;
}

function fileView() {
  return VIEW_IDS.SUMMARY_OUTPUT_FILE;
}

/**
 * `instances.query` result-set id for slim operation-summary pages (must match `with` / `select` keys).
 */
export const SUMMARY_LIST_QUERY_RESULT_SET = "summaries";

/**
 * CogniteFile (`cdf_cdm` / `v1`) properties for operation list rows — must match the view schema (no `metadata` on core CogniteFile).
 * Keeps name, description (OPS bitmap / legacy content types), mimeType, sort field; omits `sourceContext` / `source` to shrink payloads.
 */
export function getCogniteFileViewPropertiesSlim(): readonly string[] {
  return getAppConfig().dataModel.summaryListViewProperties;
}

/** Fixed order for `OPS:` bitmap line (must match save paths). */
export const SUMMARY_CONTENT_TYPE_BITMAP_ORDER: readonly DataTypeName[] = [
  "assets",
  "files",
  "work_orders",
  "time_series",
  "equipment",
];

/**
 * Leading line for CogniteFile `description` so `instances.list` can filter by
 * content type using `prefix` (scalar text; `containsAny` is not valid here).
 */
export function formatSummaryContentTypeBitmapLine(
  types: readonly DataTypeName[],
): string {
  const set = new Set(types);
  const bits = SUMMARY_CONTENT_TYPE_BITMAP_ORDER.map((t) =>
    set.has(t) ? "1" : "0",
  ).join("");
  return `OPS:${bits}\n`;
}

const OPS_BITMAP_LINE_RE = /^OPS:[01]+\n/;

export function stripSummaryContentTypeBitmapLine(text: string): string {
  return text.replace(OPS_BITMAP_LINE_RE, "");
}

function summaryListSortPropertyPath(): [string, string, string] {
  const view = fileView();
  return [view.space, `${view.externalId}/${view.version}`, "uploadedTime"];
}

export type SummaryListVariant = "operation" | "test";

export interface SummaryServerListFilters {
  dateFilterEnabled: boolean;
  dateRange: { start: Date; end: Date } | null;
  contentTypeFilterEnabled: boolean;
  selectedContentTypes: DataTypeName[];
}

function externalIdPrefixForVariant(variant: SummaryListVariant): string {
  return variant === "operation"
    ? getOperationSummaryExternalIdPrefix()
    : getTestReportExternalIdPrefix();
}

/** Stable key for React Query (server filter fingerprint). */
export function buildSummaryServerFilterFingerprint(
  filters: SummaryServerListFilters,
): string {
  const parts: string[] = [];
  if (filters.dateFilterEnabled && filters.dateRange) {
    parts.push(
      "d",
      filters.dateRange.start.toISOString(),
      filters.dateRange.end.toISOString(),
    );
  }
  if (filters.contentTypeFilterEnabled && filters.selectedContentTypes.length) {
    parts.push("c", [...filters.selectedContentTypes].sort().join(","));
  }
  return parts.length ? parts.join("|") : "none";
}

/**
 * Base filter: correct space + externalId prefix (+ optional server-side date).
 * Content-type filtering is applied client-side after `instances.list` so legacy
 * rows (no `OPS:` line) still match using markdown / description heuristics.
 */
export function buildSummaryListFilter(
  variant: SummaryListVariant,
  filters: SummaryServerListFilters,
): FilterDefinition {
  const and: FilterDefinition[] = [
    {
      equals: {
        property: ["node", "space"],
        value: getSummarySpace(),
      },
    },
    {
      prefix: {
        property: ["node", "externalId"],
        value: externalIdPrefixForVariant(variant),
      },
    },
  ];

  if (filters.dateFilterEnabled && filters.dateRange) {
    const startMs = filters.dateRange.start.getTime();
    const endMs = filters.dateRange.end.getTime();
    and.push({
      range: {
        property: ["node", "createdTime"],
        gte: startMs,
        lte: endMs,
      },
    });
  }

  return { and };
}

function buildFileInstancesQueryRequest(params: {
  variant: SummaryListVariant;
  limit: number;
  cursor?: string;
  filters: SummaryServerListFilters;
}): QueryRequest {
  const viewId = fileView();
  const filter = buildSummaryListFilter(
    params.variant,
    params.filters,
  ) as TableExpressionFilterDefinition;

  const req: QueryRequest = {
    with: {
      [SUMMARY_LIST_QUERY_RESULT_SET]: {
        nodes: {
          filter,
        },
      },
    },
    select: {
      [SUMMARY_LIST_QUERY_RESULT_SET]: {
        limit: params.limit,
        sort: [
          {
            property: [...summaryListSortPropertyPath()],
            direction: "descending",
          },
        ],
        sources: [
          {
            source: {
              type: "view",
              space: viewId.space,
              externalId: viewId.externalId,
              version: viewId.version,
            },
            properties: [...getCogniteFileViewPropertiesSlim()],
          },
        ],
      },
    },
  };

  if (params.cursor) {
    req.cursors = {
      [SUMMARY_LIST_QUERY_RESULT_SET]: params.cursor,
    };
  }

  return req;
}

/**
 * Slim operation-summary page via `instances.query` (property projection on CogniteFile).
 * Always uses the operation-summary external-id prefix.
 */
export function buildSummaryInstancesQueryRequest(params: {
  limit: number;
  cursor?: string;
  filters: SummaryServerListFilters;
}): QueryRequest {
  return buildFileInstancesQueryRequest({ ...params, variant: "operation" });
}

/**
 * Slim test-report page via `instances.query`. Same shape as
 * {@link buildSummaryInstancesQueryRequest}, with the test-report external-id prefix.
 * Stats backfill still lists full nodes because scores live in sourceContext.
 */
export function buildTestReportInstancesQueryRequest(params: {
  limit: number;
  cursor?: string;
  filters: SummaryServerListFilters;
}): QueryRequest {
  return buildFileInstancesQueryRequest({ ...params, variant: "test" });
}

/** Normalize `instances.query` response for {@link SUMMARY_LIST_QUERY_RESULT_SET}. */
export function parseSummaryQueryPageResponse(response: QueryResponse): {
  items: unknown[];
  nextCursor: string | undefined;
} {
  const key = SUMMARY_LIST_QUERY_RESULT_SET;
  const items = response.items?.[key] ?? [];
  const rawNext = response.nextCursor?.[key];
  const nextCursor =
    typeof rawNext === "string" && rawNext.length > 0 ? rawNext : undefined;
  return { items, nextCursor };
}

export function buildSummaryListRequest(params: {
  variant: SummaryListVariant;
  limit: number;
  cursor?: string;
  filters: SummaryServerListFilters;
}): SummaryInstanceListRequest {
  const viewId = fileView();
  return {
    instanceType: "node",
    sources: [
      {
        source: {
          type: "view",
          space: viewId.space,
          externalId: viewId.externalId,
          version: viewId.version,
        },
      },
    ],
    filter: buildSummaryListFilter(params.variant, params.filters),
    limit: params.limit,
    sort: [
      {
        property: [...summaryListSortPropertyPath()],
        direction: "descending",
      },
    ],
    ...(params.cursor ? { cursor: params.cursor } : {}),
  };
}

export function buildLatestSummaryListRequest(params: {
  variant: SummaryListVariant;
}): SummaryInstanceListRequest {
  return buildSummaryListRequest({
    variant: params.variant,
    limit: 1,
    filters: {
      dateFilterEnabled: false,
      dateRange: null,
      contentTypeFilterEnabled: false,
      selectedContentTypes: [],
    },
  });
}

const PICKER_EMPTY_FILTERS: SummaryServerListFilters = {
  dateFilterEnabled: false,
  dateRange: null,
  contentTypeFilterEnabled: false,
  selectedContentTypes: [],
};

/**
 * Load up to maxItems operation summaries for dropdowns (cursor chain, bounded).
 */
export async function fetchOperationSummariesForPicker(
  sdk: Pick<CogniteClient, "instances">,
  maxItems: number,
): Promise<{ items: unknown[] }> {
  const out: unknown[] = [];
  let cursor: string | undefined;

  do {
    const take = Math.min(getInstanceQueryLimit(), maxItems - out.length);
    if (take <= 0) break;

    const req = buildSummaryInstancesQueryRequest({
      limit: take,
      cursor,
      filters: PICKER_EMPTY_FILTERS,
    });
    const response = await withCdfConcurrency(() => sdk.instances.query(req));
    const { items: batch, nextCursor: next } =
      parseSummaryQueryPageResponse(response);
    out.push(...batch);
    cursor = next;
  } while (cursor && out.length < maxItems);

  return { items: out };
}
