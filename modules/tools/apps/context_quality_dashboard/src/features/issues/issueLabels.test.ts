import { describe, expect, it } from 'vitest';

import { issueKindLabelKey, issueTierBadgeVariant, issueTierLabelKey } from './issueLabels';

describe('issueLabels', () => {
  it('maps issue kinds to copy keys', () => {
    expect(issueKindLabelKey('orphan-asset')).toBe('issues.kind.orphan-asset');
    expect(issueKindLabelKey('pending-review')).toBe('issues.kind.pending-review');
  });

  it('maps tiers to criticality copy keys', () => {
    expect(issueTierLabelKey(1)).toBe('overview.criticality.critical');
    expect(issueTierLabelKey(2)).toBe('overview.criticality.important');
    expect(issueTierLabelKey(3)).toBe('overview.criticality.standard');
  });

  it('labels a missing tier Unassigned and never as the tier 3 (standard) copy', () => {
    expect(issueTierLabelKey(null)).toBe('tier.unassigned');
    expect(issueTierLabelKey(null)).not.toBe(issueTierLabelKey(3));
    expect(issueTierBadgeVariant(null)).toBe('secondary');
  });

  it('maps tiers to badge variants', () => {
    expect(issueTierBadgeVariant(1)).toBe('error');
    expect(issueTierBadgeVariant(2)).toBe('warning');
    expect(issueTierBadgeVariant(3)).toBe('secondary');
  });
});
