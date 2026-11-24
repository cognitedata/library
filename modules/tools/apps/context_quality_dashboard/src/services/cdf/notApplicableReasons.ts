import type { NotApplicableReasonDto, NotApplicableReasonKind, RunWarningDto } from '../../types';

const KINDS: readonly NotApplicableReasonKind[] = ['mapping-empty', 'all-excluded', 'no-eligible-entities'];

function isKind(value: string): value is NotApplicableReasonKind {
  return KINDS.some((kind) => kind === value);
}

/** `mapping-empty:tierByTypeCode` -> { kind: 'mapping-empty', mapping: 'tierByTypeCode' }. Unknown reasons give null. */
export function parseNotApplicableReason(reason: string | null): NotApplicableReasonDto | null {
  if (reason === null) {
    return null;
  }
  const [kind = '', mapping] = reason.split(':', 2);
  return isKind(kind) ? { kind, mapping: mapping === undefined || mapping === '' ? null : mapping } : null;
}

/**
 * The reason the run's `metric-not-applicable` warning gives for a metric. The warning's subject is the metric id, with
 * `/subKey` for a metric written per sub-key. A metric with several reasons across units gets the first the run lists.
 */
export function notApplicableReasonFor(
  warnings: readonly RunWarningDto[],
  metricId: string,
  subKey: string | undefined
): NotApplicableReasonDto | null {
  const subjects = subKey === undefined ? [metricId] : [`${metricId}/${subKey}`, metricId];
  for (const subject of subjects) {
    const match = warnings.find((warning) => warning.code === 'metric-not-applicable' && warning.subject === subject);
    const reason = match === undefined ? null : parseNotApplicableReason(match.reason);
    if (reason !== null) {
      return reason;
    }
  }
  return null;
}
