import { ALL_DIMENSION_IDS } from '../../domain/dimensions';
import type { DimensionId, MetricId } from '../../types';

import { BackendError } from './backendErrors';
import {
  parseAggregateRow,
  parseClassificationRow,
  parseDrilldownRow,
  parseExclusionRow,
  parseExtraLinkHistoryRow,
  parseExtraLinkGapRow,
  parseLocationRow,
  parseExtraLinkRow,
  type ExtraLinkHistoryRow,
  parseRunRow,
  type AggregateRow,
  type ClassificationRow,
  type DrilldownRow,
  type ExclusionRow,
  type ExtraLinkGapRow,
  type LocationRow,
  type ExtraLinkRow,
  type RunRow,
} from './metricRows';
import type { PropertyBag, RecordsClient } from './recordsClient';

/** Splitting by tier must also reach the rows that carry none (`null`), or an untiered row would vanish from a split query. */
const TIERS: readonly (number | null)[] = [1, 2, 3, null];

/** Used only to split an oversized query; kinds outside this list still come back in the unsplit query. */
const CLASSIFICATION_KINDS: readonly string[] = ['assetType', 'assetClass', 'equipmentType', 'fileCategory'];

/** A `null` value selects the rows that do not have the property. */
type Splitter = { property: string; values: readonly (string | number | null)[] };

export type RunInfo = RunRow;

/**
 * The runs the dashboard reads each part of its numbers from. A run can measure some dimensions only, so there is no
 * single current run: a dimension is current as of the latest completed run that measured it, and the configured
 * metrics as of the latest that measured them. `latest` is the latest completed run of any scope.
 */
export type CurrentRuns = {
  latest: RunInfo;
  byDimension: ReadonlyMap<DimensionId, RunInfo>;
  configured: RunInfo | null;
};

/** The configured metrics, each from the latest completed run that measured it, and which run that is. */
export type CurrentExtraLinks = {
  rows: ExtraLinkHistoryRow[];
  /** The run each metric is current as of, by `extraLinkKey`. */
  runOf: ReadonlyMap<string, RunInfo>;
};

export function extraLinkKey(link: { model: string; view: string; property: string }): string {
  return `${link.model}/${link.view}.${link.property}`;
}

/**
 * Reads the immutable metrics log. "Current" is the latest run that has a `complete` marker;
 * rows from runs without that marker (half-written or failed) are never returned.
 */
export class QualityStreamReader {
  private readonly client: RecordsClient;
  private readonly now: () => number;
  private readonly cacheMs: number;
  private runsCache: { at: number; value: Promise<RunRow[]> } | null = null;

  constructor(client: RecordsClient, now: () => number, cacheMs = 30_000) {
    this.client = client;
    this.now = now;
    this.cacheMs = cacheMs;
  }

  invalidate(): void {
    this.runsCache = null;
  }

  /** All completed runs, oldest first. */
  completedRuns(): Promise<RunRow[]> {
    const cached = this.runsCache;
    if (cached !== null && this.now() - cached.at < this.cacheMs) {
      return cached.value;
    }
    const value = this.fetchRows({ rowKind: 'run', status: 'complete' }, []).then((bags) =>
      bags
        .map(parseRunRow)
        .filter((row): row is RunRow => row !== null)
        .sort((left, right) => left.runTimestamp.localeCompare(right.runTimestamp))
    );
    this.runsCache = { at: this.now(), value };
    value.catch(() => {
      this.runsCache = null;
    });
    return value;
  }

  /** The run each dimension, and the configured metrics, are current as of. Throws when no run has completed. */
  async currentRuns(): Promise<CurrentRuns> {
    const runs = await this.completedRuns();
    const latest = runs[runs.length - 1];
    if (latest === undefined) {
      throw await this.noCompletedRun();
    }
    const byDimension = new Map<DimensionId, RunInfo>();
    let configured: RunInfo | null = null;
    // oldest first, so a newer run that covers a dimension replaces an older one
    for (const run of runs) {
      for (const dimensionId of run.coverage.dimensions) {
        byDimension.set(dimensionId, run);
      }
      if (run.coverage.configured) {
        configured = run;
      }
    }
    return { latest, byDimension, configured };
  }

  async latestRun(): Promise<RunInfo> {
    const runs = await this.completedRuns();
    const latest = runs[runs.length - 1];
    if (latest === undefined) {
      throw await this.noCompletedRun();
    }
    return latest;
  }

  /** Tells an empty stream from one that only holds unfinished runs. Runs only on the failure path. */
  private async noCompletedRun(): Promise<BackendError> {
    const message = 'No completed quality run was found in the metrics stream.';
    try {
      const anyRun = await this.fetchRows({ rowKind: 'run' }, []);
      return new BackendError('no-completed-run', message, { detail: anyRun.length === 0 ? 'empty' : 'incomplete' });
    } catch {
      return new BackendError('no-completed-run', message);
    }
  }

  async aggregates(runId: string): Promise<AggregateRow[]> {
    const bags = await this.fetchRows({ rowKind: 'aggregate', runId }, [
      { property: 'dimensionId', values: ALL_DIMENSION_IDS },
      { property: 'tier', values: TIERS },
    ]);
    return bags.map(parseAggregateRow).filter((row): row is AggregateRow => row !== null);
  }

  async classifications(runId: string): Promise<ClassificationRow[]> {
    const bags = await this.fetchRows({ rowKind: 'classification', runId }, [
      { property: 'classificationKind', values: CLASSIFICATION_KINDS },
      { property: 'tier', values: TIERS },
    ]);
    return bags.map(parseClassificationRow).filter((row): row is ClassificationRow => row !== null);
  }

  /** The metrics the run was asked to measure by its config file. Empty for a run that had none. */
  async extraLinks(runId: string): Promise<ExtraLinkRow[]> {
    const bags = await this.fetchRows({ rowKind: 'extraLink', runId }, []);
    return bags.map(parseExtraLinkRow).filter((row): row is ExtraLinkRow => row !== null);
  }

  /**
   * The configured metrics as they are current: each from the latest run that measured it, so a run of some of them
   * leaves the others as an earlier run measured them. Only the runs that say they measured configured metrics count;
   * when none does (runs written before scopes existed), the latest run is where they are read from.
   */
  async currentExtraLinks(): Promise<CurrentExtraLinks> {
    const { latest } = await this.currentRuns();
    const runs = await this.completedRuns();
    const measuring = runs.filter((run) => run.coverage.configured);
    const candidates = measuring.length === 0 ? [latest] : measuring;
    const order = new Map(candidates.map((run, index) => [run.runId, index]));
    const bags = await this.fetchRows({ rowKind: 'extraLink' }, [
      { property: 'runId', values: candidates.map((run) => run.runId) },
    ]);
    const newest = new Map<string, ExtraLinkHistoryRow>();
    const parsed = bags
      .map(parseExtraLinkHistoryRow)
      .filter((row): row is ExtraLinkHistoryRow => row !== null && order.has(row.runId));
    for (const row of parsed) {
      const key = extraLinkKey(row);
      const held = newest.get(key);
      if (held === undefined || (order.get(row.runId) ?? -1) > (order.get(held.runId) ?? -1)) {
        newest.set(key, row);
      }
    }
    const rows = parsed.filter((row) => newest.get(extraLinkKey(row))?.runId === row.runId);
    const runOf = new Map<string, RunInfo>();
    for (const [key, row] of newest) {
      const run = candidates.find((entry) => entry.runId === row.runId);
      if (run !== undefined) {
        runOf.set(key, run);
      }
    }
    return { rows, runOf };
  }

  /** Extra-link rows for every completed run, for configured-metric trend charts. */
  async extraLinkHistory(): Promise<ExtraLinkHistoryRow[]> {
    const [runs, bags] = await Promise.all([
      this.completedRuns(),
      this.fetchRows({ rowKind: 'extraLink' }, []),
    ]);
    const completed = new Set(runs.map((run) => run.runId));
    return bags
      .map(parseExtraLinkHistoryRow)
      .filter((row): row is ExtraLinkHistoryRow => row !== null && completed.has(row.runId));
  }

  /**
   * The instances without the link for one configured metric. A page that fills up is split by facility, using the
   * facilities the metric's own count rows name, so a long list is read in parts instead of failing.
   */
  async extraLinkGaps(
    runId: string,
    key: { model: string; view: string; property: string },
    facilityIds: readonly (string | null)[]
  ): Promise<ExtraLinkGapRow[]> {
    const bags = await this.fetchRows(
      { rowKind: 'extraLinkGap', runId, dimensionId: `${key.model}/${key.view}`, metricId: key.property },
      [{ property: 'facilityId', values: facilityIds }]
    );
    return bags.map(parseExtraLinkGapRow).filter((row): row is ExtraLinkGapRow => row !== null);
  }

  async locations(runId: string): Promise<LocationRow[]> {
    const bags = await this.fetchRows({ rowKind: 'location', runId }, []);
    return bags.map(parseLocationRow).filter((row): row is LocationRow => row !== null);
  }

  async exclusions(runId: string): Promise<ExclusionRow[]> {
    const bags = await this.fetchRows({ rowKind: 'exclusion', runId }, []);
    return bags.map(parseExclusionRow).filter((row): row is ExclusionRow => row !== null);
  }

  /**
   * At most `limit` drill-down rows of one metric, from one query that is never split: `cut` says there are more. A
   * facility narrows the rows on the server, so a small facility is not crowded out by a big one.
   */
  async drilldownSample(
    runId: string,
    metricId: MetricId,
    limit: number,
    facilityId?: string
  ): Promise<{ rows: DrilldownRow[]; cut: boolean }> {
    const page = await this.client.query({
      equals: { rowKind: 'drilldown', runId, metricId, ...(facilityId === undefined ? {} : { facilityId }) },
      limit,
    });
    return {
      rows: page.rows.map(parseDrilldownRow).filter((row): row is DrilldownRow => row !== null),
      cut: page.truncated,
    };
  }

  /** One metric across every completed run, for trend charts. */
  async history(metricId: MetricId): Promise<AggregateRow[]> {
    const [runs, bags] = await Promise.all([
      this.completedRuns(),
      this.fetchRows({ rowKind: 'aggregate', metricId }, [{ property: 'tier', values: TIERS }]),
    ]);
    const completed = new Set(runs.map((run) => run.runId));
    return bags
      .map(parseAggregateRow)
      .filter((row): row is AggregateRow => row !== null && completed.has(row.runId));
  }

  private async fetchRows(
    equals: Readonly<Record<string, string | number>>,
    splitters: readonly Splitter[],
    absent: readonly string[] = []
  ): Promise<PropertyBag[]> {
    const page = await this.client.query({ equals, ...(absent.length > 0 ? { absent } : {}) });
    if (!page.truncated) {
      return [...page.rows];
    }
    const [next, ...rest] = splitters;
    if (next === undefined) {
      throw new Error('A records query returned the maximum page size and cannot be split further.');
    }
    const parts = await Promise.all(
      next.values.map((value) =>
        value === null
          ? this.fetchRows(equals, rest, [...absent, next.property])
          : this.fetchRows({ ...equals, [next.property]: value }, rest, absent)
      )
    );
    return parts.flat();
  }
}
