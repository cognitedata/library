import { interpolate } from '../../i18n/interpolate';
import type { RunWarningDto } from '../../types';

export type RunNote = {
  id: string;
  code: string;
  text: string;
  /** Type codes the note lists, as one line. Never entity ids. */
  detail: string | null;
};

type Translate = (key: string) => string;

function reasonText(reason: string | null, t: Translate): string {
  const [kind = '', mapping] = (reason ?? '').split(':', 2);
  if (kind === 'mapping-empty') {
    return mapping === undefined || mapping === ''
      ? t('dimension.metric.notApplicable.mapping-empty.generic')
      : interpolate(t('dimension.metric.notApplicable.mapping-empty'), { mapping });
  }
  if (kind === 'all-excluded' || kind === 'no-eligible-entities') {
    return t(`dimension.metric.notApplicable.${kind}`);
  }
  return t('dimension.metric.notApplicable');
}

/** A metric id is shown by its label when the dashboard has one; a sub-key is kept after a slash. */
function subjectText(subject: string | null, t: Translate): string {
  if (subject === null) {
    return '';
  }
  const [metricId = '', subKey] = subject.split('/', 2);
  const label = t(`metric.${metricId}`);
  const name = label === `metric.${metricId}` ? metricId : label;
  return subKey === undefined ? name : `${name} / ${subKey}`;
}

function textFor(warning: RunWarningDto, t: Translate): string {
  const missing = t('runNotes.missingCount');
  const counts: Record<string, string> = {};
  for (const [name, value] of Object.entries(warning.counts)) {
    counts[name] = String(value);
  }
  const key = `runNotes.code.${warning.code}`;
  const template = t(key);
  if (template === key) {
    return warning.message === ''
      ? interpolate(t('runNotes.unknownNoMessage'), { code: warning.code })
      : interpolate(t('runNotes.unknown'), { message: warning.message });
  }
  const subject =
    warning.code === 'metric-not-applicable' || warning.code === 'drilldown-truncated'
      ? subjectText(warning.subject, t)
      : (warning.subject ?? '');
  return interpolate(
    template,
    { ...counts, subject, reason: reasonText(warning.reason, t), message: warning.message },
    missing
  );
}

/** The run's warnings as sentences, in the order the run wrote them, with the "did not fit" note last. */
export function presentRunNotes(warnings: readonly RunWarningDto[], t: Translate): RunNote[] {
  const notes = warnings.map((warning, index): RunNote => ({
    id: `${index}:${warning.code}:${warning.subject ?? ''}:${warning.reason ?? ''}`,
    code: warning.code,
    text: textFor(warning, t),
    detail: warning.codes.length === 0 ? null : interpolate(t('runNotes.codes'), { codes: warning.codes.join(', ') }),
  }));
  return [...notes.filter((note) => note.code !== 'warnings-truncated'), ...notes.filter((note) => note.code === 'warnings-truncated')];
}
