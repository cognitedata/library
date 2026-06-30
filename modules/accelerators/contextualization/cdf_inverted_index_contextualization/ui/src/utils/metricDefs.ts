import type { MessageKey } from "../i18n";

export const BUILD_METRICS: { key: string; labelKey: MessageKey }[] = [
  { key: "dry_run", labelKey: "metrics.dryRun" },
  { key: "candidate_entries", labelKey: "metrics.candidateEntries" },
  { key: "registry_scopes", labelKey: "metrics.registryScopes" },
  { key: "entries_written", labelKey: "metrics.entriesWritten" },
  { key: "entries_upserted", labelKey: "metrics.entriesUpserted" },
  { key: "partitions_touched", labelKey: "metrics.partitionsTouched" },
  { key: "scopes_processed", labelKey: "metrics.scopesProcessed" },
  { key: "views_processed", labelKey: "metrics.viewsProcessed" },
  { key: "instances_scanned", labelKey: "metrics.instancesScanned" },
  { key: "dry_run_results_count", labelKey: "metrics.dryRunResultsCount" },
  { key: "duration_seconds", labelKey: "metrics.durationSeconds" },
];

export const QUERY_SUMMARY_METRICS: { key: string; labelKey: MessageKey }[] = [
  { key: "hit_count", labelKey: "metrics.hitCount" },
  { key: "terms_queried", labelKey: "metrics.termsQueried" },
  { key: "scopes_queried", labelKey: "metrics.scopesQueried" },
];

export const REUSE_METRICS: { key: string; labelKey: MessageKey }[] = [
  { key: "terms_with_hits", labelKey: "metrics.termsWithHits" },
  { key: "cross_scope_duplicate_count", labelKey: "metrics.crossScopeDuplicates" },
  { key: "cross_scope_duplicate_rate", labelKey: "metrics.crossScopeDuplicateRate" },
  { key: "by_term_count", labelKey: "metrics.byTermCount" },
];

export const TAG_REUSE_METRICS: { key: string; labelKey: MessageKey }[] = [
  { key: "scopes_scanned", labelKey: "metrics.scopesScanned" },
  { key: "lookup_keys_scanned", labelKey: "metrics.lookupKeysScanned" },
  { key: "unique_terms_scanned", labelKey: "metrics.uniqueTermsScanned" },
  { key: "min_scope_count", labelKey: "metrics.minScopeCount" },
  ...REUSE_METRICS,
];

export const TARGET_DRIVEN_METRICS: { key: string; labelKey: MessageKey }[] = [
  { key: "processed", labelKey: "metrics.processed" },
  { key: "references_found", labelKey: "metrics.referencesFound" },
  { key: "links_created", labelKey: "metrics.linksCreated" },
  { key: "dedupe_skipped", labelKey: "metrics.dedupeSkipped" },
  { key: "skipped", labelKey: "metrics.skipped" },
  { key: "dry_run", labelKey: "metrics.dryRun" },
];
