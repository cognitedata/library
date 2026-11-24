import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { RunWarningDto } from '../../types';

import { presentRunNotes } from './runNotePresentation';

const t = (key: string): string => translate(key, 'en');

function warning(partial: Partial<RunWarningDto> & { code: string }): RunWarningDto {
  return { message: '', subject: null, reason: null, counts: {}, codes: [], ...partial };
}

describe(presentRunNotes.name, () => {
  it('writes a sentence with the counts for each known code', () => {
    const notes = presentRunNotes(
      [
        warning({ code: 'unmapped-asset-types', counts: { untyped: 3, unmapped: 2 }, codes: ['widget', 'gadget'] }),
        warning({ code: 'parents-not-loaded', counts: { assets: 4 } }),
        warning({ code: 'drilldown-truncated', subject: 'orphansByTier', counts: { limit: 500, dropped: 20, units: 1 } }),
        warning({ code: 'warnings-truncated', counts: { dropped: 2 } }),
      ],
      t
    );

    const text = notes.map((note) => note.text);
    expect(text).toContain('3 assets have no type and 2 have a type that is not mapped to a tier, so they have no tier.');
    expect(text).toContain('4 assets have a parent outside the loaded data. They count as having a parent, and their depth is a lower bound.');
    expect(text.some((line) => line.includes('cut at 500 per unit (20 not listed)'))).toBe(true);
    expect(text).toContain('2 more notes did not fit and are not shown.');
    expect(notes[0]?.detail).toBe('Types: widget, gadget');
  });

  it('names the empty mapping and the metric, with the reason, for configuration and not-applicable notes', () => {
    const notes = presentRunNotes(
      [
        warning({ code: 'mapping-empty', subject: 'tierByTypeCode' }),
        warning({ code: 'metric-not-applicable', subject: 'monitoringCoverage', reason: 'mapping-empty:tierByTypeCode', counts: { units: 2 } }),
        warning({ code: 'metric-not-applicable', subject: 'approvalMix/Approved', reason: 'all-excluded', counts: { units: 1 } }),
      ],
      t
    );

    expect(notes[0]?.text).toBe('tierByTypeCode is not configured, so the metrics that need it are not applicable.');
    expect(notes[1]?.text).toContain('2 reporting unit(s)');
    expect(notes[1]?.text).toContain('tierByTypeCode is not configured');
    expect(notes[2]?.text).toContain('Every entity was excluded');
  });

  it('shows a generic note with the backend message for an unknown code, and the code when there is no message', () => {
    const [withMessage, without] = presentRunNotes(
      [warning({ code: 'from-the-future', message: 'Something new' }), warning({ code: 'also-new' })],
      t
    );

    expect(withMessage?.text).toBe('Note: Something new');
    expect(without?.text).toBe('Note (also-new)');
  });

  it('shows legacy free text as it is', () => {
    expect(presentRunNotes([warning({ code: 'free-text', message: '3 assets had no type' })], t)[0]?.text).toBe('3 assets had no type');
  });

  it('does not print a placeholder or crash when a count is missing', () => {
    const [note] = presentRunNotes([warning({ code: 'parents-not-loaded' })], t);

    expect(note?.text).toBe('some assets have a parent outside the loaded data. They count as having a parent, and their depth is a lower bound.');
    expect(note?.text).not.toContain('{{');
  });

  it('never shows anything but counts, subjects, reasons and type codes', () => {
    const [note] = presentRunNotes([warning({ code: 'parents-not-loaded', counts: { assets: 1 } })], t);

    expect(JSON.stringify(note)).not.toMatch(/asset-\d|entity/i);
  });

  it('gives every note a stable id and keeps the run order, with the truncation note last', () => {
    const notes = presentRunNotes(
      [warning({ code: 'warnings-truncated', counts: { dropped: 1 } }), warning({ code: 'mapping-empty', subject: 'a' }), warning({ code: 'mapping-empty', subject: 'b' })],
      t
    );

    expect(notes.map((note) => note.id)).toHaveLength(new Set(notes.map((note) => note.id)).size);
    expect(notes[notes.length - 1]?.code).toBe('warnings-truncated');
  });
});
