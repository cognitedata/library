export function formatLastSuccessfulRun(isoTimestamp: string, locale: string): string {
  return new Date(isoTimestamp).toLocaleString(locale, { timeZone: 'UTC' });
}
