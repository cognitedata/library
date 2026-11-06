import { describe, expect, it } from 'vitest';

import { KNOWN_WARNING_CODES, MAX_WARNINGS, parseRunWarnings } from './runWarnings';

const sample = JSON.stringify([
  { code: 'mapping-empty', message: 'tierByTypeCode is empty', subject: 'tierByTypeCode' },
  { code: 'unmapped-asset-types', message: 'm', untyped: 3, unmapped: 2, codes: ['widget', 'gadget'] },
  { code: 'metric-not-applicable', message: 'm', subject: 'monitoringCoverage', reason: 'mapping-empty:tierByTypeCode', units: 2 },
]);

describe(parseRunWarnings.name, () => {
  it('reads the backend warnings JSON text into warnings with counts, subject, reason and codes', () => {
    const warnings = parseRunWarnings(sample);

    expect(warnings).toHaveLength(3);
    expect(warnings[0]).toMatchObject({ code: 'mapping-empty', subject: 'tierByTypeCode', counts: {} });
    expect(warnings[1]).toMatchObject({ code: 'unmapped-asset-types', counts: { untyped: 3, unmapped: 2 }, codes: ['widget', 'gadget'] });
    expect(warnings[2]).toMatchObject({ code: 'metric-not-applicable', reason: 'mapping-empty:tierByTypeCode', counts: { units: 2 } });
  });

  it.each([undefined, null, '', '   ', '[]', 'null', '{}', 7, true, {}])('gives no warnings for %j', (value) => {
    expect(parseRunWarnings(value)).toEqual([]);
  });

  it('keeps a legacy free-text value as one plain note instead of failing', () => {
    expect(parseRunWarnings('3 assets had no type')).toEqual([
      expect.objectContaining({ code: 'free-text', message: '3 assets had no type', counts: {}, codes: [] }),
    ]);
  });

  it('keeps an unknown code and ignores fields it does not know', () => {
    const [warning] = parseRunWarnings(JSON.stringify([{ code: 'brand-new-code', message: 'hello', mystery: { a: 1 }, assets: 4, entityId: 'asset-77' }]));

    expect(warning).toMatchObject({ code: 'brand-new-code', message: 'hello', counts: { assets: 4 } });
    expect(JSON.stringify(warning)).not.toContain('asset-77');
    expect(JSON.stringify(warning)).not.toContain('mystery');
  });

  it('skips entries that are not warning objects and never throws', () => {
    const warnings = parseRunWarnings(JSON.stringify([1, 'x', null, [], { message: 'no code' }, { code: 5 }, { code: 'ok', message: 'fine' }]));

    expect(warnings.map((warning) => warning.code)).toEqual(['ok']);
  });

  it('reads one warning object as a list of one', () => {
    expect(parseRunWarnings(JSON.stringify({ code: 'no-source-entities', message: 'none', subject: 'files' }))).toHaveLength(1);
  });

  it('ignores a non-numeric or non-finite count and a non-string subject, reason or code list entry', () => {
    const [warning] = parseRunWarnings(JSON.stringify([{ code: 'x', message: 'm', units: 'many', dropped: 3, subject: 4, reason: {}, codes: ['a', 2, 'b'] }]));

    expect(warning).toMatchObject({ subject: null, reason: null, counts: { dropped: 3 }, codes: ['a', 'b'] });
  });

  it('bounds the number of warnings, the text length and the code list', () => {
    const many = Array.from({ length: 200 }, (_, index) => ({ code: `c${index}`, message: 'm'.repeat(5000), codes: Array.from({ length: 100 }, (_, i) => `t${i}`) }));

    const warnings = parseRunWarnings(JSON.stringify(many));

    expect(warnings).toHaveLength(MAX_WARNINGS);
    expect(warnings[0]?.message.length).toBeLessThanOrEqual(300);
    expect(warnings[0]?.codes.length).toBeLessThanOrEqual(10);
  });

  it('keeps the truncation note the backend appends when the warnings did not fit', () => {
    const [warning] = parseRunWarnings(JSON.stringify([{ code: 'warnings-truncated', dropped: 4, message: 'More warnings than fit; see a dry run.' }]));

    expect(warning).toMatchObject({ code: 'warnings-truncated', counts: { dropped: 4 } });
  });

  it('lists every code the backend writes', () => {
    expect([...KNOWN_WARNING_CODES].sort()).toEqual(
      [
        'mapping-empty',
        'unmapped-asset-types',
        'optional-source-unavailable',
        'no-source-entities',
        'metric-not-applicable',
        'drilldown-truncated',
        'warnings-truncated',
        'parents-not-loaded',
        'tiering-off',
        'export-failed',
        'location-unsupported',
        'locations-unavailable',
      ].sort()
    );
  });
});
