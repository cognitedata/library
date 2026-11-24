import type { DimensionId, IssueDto, MetricId, QualityIssueKind } from '../../types';

import { issueKindLabelKey } from './issueLabels';

export type IssuePatternRow = {
  kind: QualityIssueKind;
  dimensionId: DimensionId;
  metricId: MetricId;
  nameKey: string;
  /** What the kind means. It describes the whole group, never one of its items. */
  descriptionKey: string;
  affectedCount: number;
  ownerId: string;
  issueIds: readonly string[];
};

export function issuePatternRowsForDimension(
  rows: readonly IssuePatternRow[],
  dimensionId?: string
): IssuePatternRow[] {
  if (dimensionId === undefined) {
    return [...rows];
  }
  return rows.filter((row) => row.dimensionId === dimensionId);
}

export function buildIssueTypeFilterOptions(
  rows: readonly IssuePatternRow[],
  dimensionId: string | undefined,
  t: (key: string) => string
): Array<{ id: string; label: string }> {
  return uniqueKinds(issuePatternRowsForDimension(rows, dimensionId)).map((row) => ({
    id: row.kind,
    label: t(issueKindLabelKey(row.kind)),
  }));
}

export function mergeIssuePatternFilters(
  previous: { dimensionId?: string; kind?: string },
  next: { dimensionId?: string; kind?: string; ownerId?: string; search: string },
  rows: readonly IssuePatternRow[]
): typeof next {
  if (next.dimensionId === previous.dimensionId) {
    return next;
  }
  const scopedKinds = new Set(
    issuePatternRowsForDimension(rows, next.dimensionId).map((row) => row.kind)
  );
  const kind =
    next.kind !== undefined && scopedKinds.has(next.kind as QualityIssueKind) ? next.kind : undefined;
  return { ...next, kind };
}

function uniqueKinds(rows: readonly IssuePatternRow[]): IssuePatternRow[] {
  const seen = new Set<QualityIssueKind>();
  const result: IssuePatternRow[] = [];
  for (const row of rows) {
    if (!seen.has(row.kind)) {
      seen.add(row.kind);
      result.push(row);
    }
  }
  return result;
}

function issuePatternGroupKey(issue: IssueDto): string {
  return `${issue.dimensionId}:${issue.kind}`;
}

export function buildIssuePatternRows(issues: readonly IssueDto[]): IssuePatternRow[] {
  const groups = new Map<string, IssueDto[]>();
  for (const issue of issues) {
    const key = issuePatternGroupKey(issue);
    const existing = groups.get(key);
    if (existing !== undefined) {
      existing.push(issue);
    } else {
      groups.set(key, [issue]);
    }
  }

  return [...groups.entries()]
    .map(([, groupIssues]) => {
      const kind = groupIssues[0].kind;
      return {
      kind,
      dimensionId: groupIssues[0].dimensionId,
      metricId: groupIssues[0].metricId,
      nameKey: `issues.kind.${kind}`,
      descriptionKey: `issues.kindDescription.${kind}`,
      affectedCount: groupIssues.length,
      ownerId: mostCommonOwnerId(groupIssues),
      issueIds: groupIssues.map((issue) => issue.id),
    };
    })
    .sort((left, right) => right.affectedCount - left.affectedCount);
}

function mostCommonOwnerId(issues: readonly IssueDto[]): string {
  const counts = new Map<string, number>();
  for (const issue of issues) {
    counts.set(issue.ownerId, (counts.get(issue.ownerId) ?? 0) + 1);
  }

  let bestOwnerId = issues[0].ownerId;
  let bestCount = 0;
  for (const [ownerId, count] of counts) {
    if (count > bestCount) {
      bestCount = count;
      bestOwnerId = ownerId;
    }
  }
  return bestOwnerId;
}
