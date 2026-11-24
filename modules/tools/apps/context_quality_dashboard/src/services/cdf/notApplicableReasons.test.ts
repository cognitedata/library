import { describe, expect, it } from 'vitest';

import { notApplicableReasonFor, parseNotApplicableReason } from './notApplicableReasons';

const warning = (subject: string, reason: string | null) => ({ code: 'metric-not-applicable', message: '', subject, reason, counts: {}, codes: [] });

describe(parseNotApplicableReason.name, () => {
  it('splits a mapping reason into its kind and the empty configuration key', () => {
    expect(parseNotApplicableReason('mapping-empty:tierByTypeCode')).toEqual({ kind: 'mapping-empty', mapping: 'tierByTypeCode' });
  });

  it('reads the reasons that carry no key', () => {
    expect(parseNotApplicableReason('all-excluded')).toEqual({ kind: 'all-excluded', mapping: null });
    expect(parseNotApplicableReason('no-eligible-entities')).toEqual({ kind: 'no-eligible-entities', mapping: null });
  });

  it('gives nothing for an absent or unknown reason', () => {
    expect(parseNotApplicableReason(null)).toBeNull();
    expect(parseNotApplicableReason('something-new')).toBeNull();
    expect(parseNotApplicableReason('')).toBeNull();
  });
});

describe(notApplicableReasonFor.name, () => {
  it('finds a metric by its id, and a sub-key metric by id/subKey before the bare id', () => {
    const warnings = [warning('approvalMix', 'all-excluded'), warning('approvalMix/Approved', 'no-eligible-entities')];

    expect(notApplicableReasonFor(warnings, 'approvalMix', 'Approved')?.kind).toBe('no-eligible-entities');
    expect(notApplicableReasonFor(warnings, 'approvalMix', 'Rejected')?.kind).toBe('all-excluded');
  });

  it('ignores other codes and other metrics', () => {
    expect(notApplicableReasonFor([{ ...warning('orphanRate', 'all-excluded'), code: 'mapping-empty' }], 'orphanRate', undefined)).toBeNull();
    expect(notApplicableReasonFor([warning('orphanRate', 'all-excluded')], 'freshness', undefined)).toBeNull();
  });
});
