import {
  AnnotationStatus,
  NormalizedStatus,
  StatusTags,
  CoverageThresholds,
  FileAnnotationStatus,
} from "./constants";
import type {
  AnnotationRecord,
  AnnotationState,
  CoverageData,
  GroupedCoverage,
  PipelineKPIs,
  ThresholdBucket,
  FileAggregation,
} from "./types";

/**
 * Data processing utilities for annotation and pipeline data
 */
export class DataProcessor {
  /**
   * Derive normalized status from raw annotation record
   */
  static deriveNormalizedStatus(record: AnnotationRecord): NormalizedStatus {
    const tags = record.tags;
    const rawStatus = record.status;
    const tagSet = new Set<string>();

    if (tags) {
      if (Array.isArray(tags)) {
        tags.forEach((t) => tagSet.add(String(t)));
      } else {
        String(tags)
          .split(",")
          .forEach((t) => {
            const trimmed = t.trim();
            if (trimmed) tagSet.add(trimmed);
          });
      }
    }

    let normalized: NormalizedStatus;

    if (rawStatus === AnnotationStatus.APPROVED) {
      if (tagSet.has(StatusTags.PROMOTED_AUTO)) {
        normalized = NormalizedStatus.AUTOMATICALLY_PROMOTED;
      } else if (tagSet.has(StatusTags.PROMOTED_MANUALLY)) {
        normalized = NormalizedStatus.MANUALLY_PROMOTED;
      } else {
        normalized = NormalizedStatus.REGULARLY_ANNOTATED;
      }
    } else if (!rawStatus) {
      normalized = NormalizedStatus.PATTERN_FOUND;
    } else if (rawStatus === AnnotationStatus.SUGGESTED) {
      if (
        tagSet.has(StatusTags.AMBIGUOUS_MATCH) ||
        tagSet.has(StatusTags.PROMOTE_ATTEMPTED)
      ) {
        normalized = NormalizedStatus.AMBIGUOUS;
      } else {
        normalized = NormalizedStatus.PATTERN_FOUND;
      }
    } else if (rawStatus === AnnotationStatus.REJECTED) {
      normalized = NormalizedStatus.NO_MATCH;
    } else {
      normalized = NormalizedStatus.PATTERN_FOUND;
    }

    return normalized;
  }

  /**
   * Calculate overall coverage from actual and potential annotations
   */
  static calculateCoverage(
    actualRecords: AnnotationRecord[],
    potentialRecords: AnnotationRecord[]
  ): CoverageData {
    const actualCount = actualRecords.length;
    const potentialCount = potentialRecords.length;
    const totalPossible = actualCount + potentialCount;
    const coveragePct =
      totalPossible > 0 ? (actualCount / totalPossible) * 100 : 0;

    return {
      coveragePct,
      actualCount,
      potentialCount,
      totalPossible,
    };
  }

  /**
   * Calculate coverage grouped by a specific field
   */
  static calculateGroupedCoverage(
    actualRecords: AnnotationRecord[],
    potentialRecords: AnnotationRecord[],
    groupByKey: keyof AnnotationRecord
  ): GroupedCoverage[] {
    const groups = new Set<string>();

    // Collect all unique group values
    actualRecords.forEach((r) => {
      const val = r[groupByKey];
      if (val != null) groups.add(String(val));
    });
    potentialRecords.forEach((r) => {
      const val = r[groupByKey];
      if (val != null) groups.add(String(val));
    });

    const results: GroupedCoverage[] = [];

    for (const groupKey of Array.from(groups).sort()) {
      const actualCount = actualRecords.filter(
        (r) => String(r[groupByKey]) === groupKey
      ).length;
      const potentialCount = potentialRecords.filter(
        (r) => String(r[groupByKey]) === groupKey
      ).length;
      const totalPossible = actualCount + potentialCount;
      const coveragePct =
        totalPossible > 0 ? (actualCount / totalPossible) * 100 : 0;

      results.push({
        groupKey,
        coveragePct,
        actualCount,
        potentialCount,
        totalPossible,
      });
    }

    return results.sort((a, b) => b.coveragePct - a.coveragePct);
  }

  /**
   * Calculate pipeline KPIs from annotation states
   */
  static calculatePipelineKPIs(states: AnnotationState[]): PipelineKPIs {
    let awaiting = 0;
    let processedTotal = 0;
    let failedTotal = 0;

    for (const state of states) {
      const status = state.annotationStatus;
      if (status === FileAnnotationStatus.AWAITING) {
        awaiting++;
      } else if (
        status === FileAnnotationStatus.ANNOTATED ||
        status === FileAnnotationStatus.FAILED
      ) {
        processedTotal++;
        if (status === FileAnnotationStatus.FAILED) {
          failedTotal++;
        }
      }
    }

    const failureRateTotal =
      processedTotal > 0 ? (failedTotal / processedTotal) * 100 : 0;

    return {
      awaitingProcessing: awaiting,
      processedTotal,
      failedTotal,
      failureRateTotal,
    };
  }

  /**
   * Calculate coverage threshold buckets for visualization
   */
  static calculateThresholdBuckets(
    files: FileAggregation[]
  ): ThresholdBucket[] {
    const total = files.length;

    const highCount = files.filter((f) => f.coveragePct >= 90).length;
    const upperCount = files.filter(
      (f) => f.coveragePct >= 75 && f.coveragePct < 90
    ).length;
    const midCount = files.filter(
      (f) => f.coveragePct >= 25 && f.coveragePct < 75
    ).length;
    const lowCount = files.filter((f) => f.coveragePct < 25).length;

    const pct = (n: number) => (total > 0 ? (n / total) * 100 : 0);

    return [
      {
        key: "high",
        label: CoverageThresholds.HIGH.label,
        count: highCount,
        percentage: pct(highCount),
        color: CoverageThresholds.HIGH.color,
        emoji: CoverageThresholds.HIGH.emoji,
      },
      {
        key: "upper",
        label: CoverageThresholds.UPPER.label,
        count: upperCount,
        percentage: pct(upperCount),
        color: CoverageThresholds.UPPER.color,
        emoji: CoverageThresholds.UPPER.emoji,
      },
      {
        key: "mid",
        label: CoverageThresholds.MID.label,
        count: midCount,
        percentage: pct(midCount),
        color: CoverageThresholds.MID.color,
        emoji: CoverageThresholds.MID.emoji,
      },
      {
        key: "low",
        label: CoverageThresholds.LOW.label,
        count: lowCount,
        percentage: pct(lowCount),
        color: CoverageThresholds.LOW.color,
        emoji: CoverageThresholds.LOW.emoji,
      },
    ];
  }

  /**
   * Aggregate annotations by file
   */
  static aggregateByFile(
    actualRecords: AnnotationRecord[],
    potentialRecords: AnnotationRecord[]
  ): FileAggregation[] {
    const fileMap = new Map<string, FileAggregation>();

    // Process actual records
    for (const record of actualRecords) {
      const fileId = record.fileExternalId || record.startNode;
      if (!fileId) continue;

      if (!fileMap.has(fileId)) {
        fileMap.set(fileId, {
          fileExternalId: fileId,
          fileName: record.fileName,
          fileSourceId: record.fileSourceId,
          fileResourceType: record.fileResourceType,
          filePrimaryScope: record.filePrimaryScope,
          fileSecondaryScope: record.fileSecondaryScope,
          actualCount: 0,
          potentialCount: 0,
          totalPossible: 0,
          coveragePct: 0,
        });
      }
      fileMap.get(fileId)!.actualCount++;
    }

    // Process potential records
    for (const record of potentialRecords) {
      const fileId = record.fileExternalId || record.startNode;
      if (!fileId) continue;

      if (!fileMap.has(fileId)) {
        fileMap.set(fileId, {
          fileExternalId: fileId,
          fileName: record.fileName,
          fileSourceId: record.fileSourceId,
          fileResourceType: record.fileResourceType,
          filePrimaryScope: record.filePrimaryScope,
          fileSecondaryScope: record.fileSecondaryScope,
          actualCount: 0,
          potentialCount: 0,
          totalPossible: 0,
          coveragePct: 0,
        });
      }
      fileMap.get(fileId)!.potentialCount++;
    }

    // Calculate totals and coverage
    for (const file of fileMap.values()) {
      file.totalPossible = file.actualCount + file.potentialCount;
      file.coveragePct =
        file.totalPossible > 0
          ? (file.actualCount / file.totalPossible) * 100
          : 0;
    }

    return Array.from(fileMap.values()).sort(
      (a, b) => a.coveragePct - b.coveragePct
    );
  }

  /**
   * Group annotations by tag and aggregate
   */
  static groupByTag(
    records: AnnotationRecord[],
    _includeSecondaryScope = false
  ): Map<
    string,
    { count: number; files: Set<string>; resourceType?: string; secondaryScope?: string; normalizedStatus?: string }
  > {
    const groups = new Map<
      string,
      { count: number; files: Set<string>; resourceType?: string; secondaryScope?: string; normalizedStatus?: string }
    >();

    for (const record of records) {
      const tag = record.startNodeText;
      if (!tag) continue;

      if (!groups.has(tag)) {
        groups.set(tag, {
          count: 0,
          files: new Set(),
          resourceType: record.endNodeResourceType,
          secondaryScope: record.fileSecondaryScope,
          normalizedStatus: record.normalizedStatus,
        });
      }

      const group = groups.get(tag)!;
      group.count++;
      const fileId = record.fileExternalId || record.startNode;
      if (fileId) group.files.add(fileId);
    }

    return groups;
  }

  /**
   * Format a number with thousands separators
   */
  static formatNumber(n: number): string {
    return n.toLocaleString();
  }

  /**
   * Format a percentage
   */
  static formatPercentage(n: number, decimals = 2): string {
    return `${n.toFixed(decimals)}%`;
  }

  /**
   * Filter records by various criteria
   */
  static filterRecords(
    records: AnnotationRecord[],
    filters: {
      resourceType?: string;
      secondaryScope?: string;
      fileExternalIds?: string[];
    }
  ): AnnotationRecord[] {
    return records.filter((r) => {
      if (
        filters.resourceType &&
        r.endNodeResourceType !== filters.resourceType
      ) {
        return false;
      }
      if (
        filters.secondaryScope &&
        r.fileSecondaryScope !== filters.secondaryScope
      ) {
        return false;
      }
      if (filters.fileExternalIds && filters.fileExternalIds.length > 0) {
        const fileId = r.fileExternalId || r.startNode;
        if (!fileId || !filters.fileExternalIds.includes(fileId)) {
          return false;
        }
      }
      return true;
    });
  }

  /**
   * Get unique values for a field (for filter options)
   */
  static getUniqueValues(
    records: AnnotationRecord[],
    field: keyof AnnotationRecord
  ): string[] {
    const values = new Set<string>();
    for (const record of records) {
      const val = record[field];
      if (val != null) {
        values.add(String(val));
      }
    }
    return Array.from(values).sort();
  }

  /**
   * Parse pipeline run message to extract metadata
   */
  static parseRunMessage(message: string): {
    caller?: string;
    functionId?: string;
    callId?: string;
    total?: number;
    success?: number;
    failed?: number;
  } {
    if (!message) return {};

    const pattern =
      /\(caller:(?<caller>\w+), function_id:(?<functionId>[\w.-]+), call_id:(?<callId>[\w.-]+)\) - total files processed: (?<total>\d+) - successful files: (?<success>\d+) - failed files: (?<failed>\d+)/;

    const match = message.match(pattern);
    if (!match || !match.groups) return {};

    return {
      caller: match.groups.caller,
      functionId: match.groups.functionId,
      callId: match.groups.callId,
      total: Number(match.groups.total),
      success: Number(match.groups.success),
      failed: Number(match.groups.failed),
    };
  }
}
