/**
 * Centralised React Query cache keys for all CDF data hooks.
 * Keeping keys in one place ensures consistent cache invalidation.
 */

export const CDF_QUERY_KEYS = {
  agents: ["agents"] as const,
  canEditAgentConfig: ["canEditAgentConfig"] as const,
  /** Prefix for invalidation — all summary-related keys start with "summaries". */
  summaries: ["summaries"] as const,
  summariesPage: (
    variant: string,
    filterFingerprint: string,
    pageSize: number,
    cursorKey: string,
  ) =>
    [
      "summaries",
      "page",
      variant,
      filterFingerprint,
      pageSize,
      cursorKey,
    ] as const,
  summariesLatest: (variant: string) =>
    ["summaries", "latest", variant] as const,
  summariesPicker: () => ["summaries", "picker", "operation"] as const,
  testReportStatsStreaming: () =>
    ["summaries", "testStats", "streaming"] as const,
  fileContent: (space: string | undefined, externalId: string | undefined) =>
    ["fileContent", space, externalId] as const,
  /** Prefix for invalidating all saved-podcast queries. */
  podcast: ["podcast"] as const,
  savedPodcast: (sourceSummaryId: string | undefined) =>
    ["podcast", "saved", sourceSummaryId] as const,
  instances: (params: unknown) => ["instances", params] as const,
  instancesByTime: (
    dataType: string,
    start: Date,
    end: Date,
    timeProp: string,
  ) => ["instances-by-time", dataType, start, end, timeProp] as const,
} as const;
