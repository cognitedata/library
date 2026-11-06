import type { QualityIssueKind, Tier } from '../../types';

export function issueKindLabelKey(kind: QualityIssueKind): string {
  return `issues.kind.${kind}`;
}

/** A null tier is an entity the backend could not tier: it is labelled Unassigned, never as tier 3. */
export function issueTierLabelKey(tier: Tier | null): string {
  if (tier === null) {
    return 'tier.unassigned';
  }
  if (tier === 1) {
    return 'overview.criticality.critical';
  }
  if (tier === 2) {
    return 'overview.criticality.important';
  }
  return 'overview.criticality.standard';
}

export function issueTierBadgeVariant(tier: Tier | null): 'error' | 'warning' | 'secondary' {
  if (tier === 1) {
    return 'error';
  }
  if (tier === 2) {
    return 'warning';
  }
  return 'secondary';
}
