import type { RunWarningDto } from '../../types';

/** The warning codes the backend writes. Any other code is shown generically. */
export const KNOWN_WARNING_CODES: readonly string[] = [
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
];

/** Code for a warnings value that is plain text rather than JSON. */
export const FREE_TEXT_WARNING_CODE = 'free-text';

export const MAX_WARNINGS = 50;
const MAX_TEXT_LENGTH = 300;
const MAX_LISTED_CODES = 10;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value: unknown): string | null {
  return typeof value === 'string' ? value.slice(0, MAX_TEXT_LENGTH) : null;
}

/** Only counts are kept: numbers other than the fields that have a place of their own. Names, ids and nested data are dropped. */
function counts(entry: Record<string, unknown>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [key, value] of Object.entries(entry)) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      out[key] = value;
    }
  }
  return out;
}

function typeCodes(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((item): item is string => typeof item === 'string')
    .slice(0, MAX_LISTED_CODES)
    .map((item) => item.slice(0, 80));
}

function toWarning(entry: unknown): RunWarningDto | null {
  if (!isRecord(entry) || typeof entry.code !== 'string' || entry.code === '') {
    return null;
  }
  return {
    code: entry.code.slice(0, 80),
    message: text(entry.message) ?? '',
    subject: text(entry.subject),
    reason: text(entry.reason),
    counts: counts(entry),
    codes: typeCodes(entry.codes),
  };
}

/**
 * The run marker's `warnings` property as warnings. It is JSON text (a list of warning objects), but a
 * run may have none (absent or empty), an older producer may have written free text, and a newer one may add codes or
 * fields: none of that may break a page. This never throws.
 */
export function parseRunWarnings(value: unknown): RunWarningDto[] {
  if (typeof value !== 'string' || value.trim() === '') {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return [
      { code: FREE_TEXT_WARNING_CODE, message: value.trim().slice(0, MAX_TEXT_LENGTH), subject: null, reason: null, counts: {}, codes: [] },
    ];
  }
  const entries: unknown[] = Array.isArray(parsed) ? parsed : [parsed];
  return entries.flatMap((entry) => {
    const warning = toWarning(entry);
    return warning === null ? [] : [warning];
  }).slice(0, MAX_WARNINGS);
}
