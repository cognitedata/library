export function formatCount(count: number): string {
  return count.toLocaleString('en-US');
}

export function formatSignedCount(count: number): string {
  if (count === 0) {
    return '0';
  }
  const sign = count > 0 ? '+' : '−';
  return `${sign}${formatCount(Math.abs(count))}`;
}

export function buildCoverageBreakdownAriaLabel(
  t: (key: string) => string,
  counts: { contextualised: number; remaining: number }
): string {
  return interpolate(t('overview.coverage.breakdownAria'), {
    contextualised: formatCount(counts.contextualised),
    remaining: formatCount(counts.remaining),
  });
}

function interpolate(template: string, vars: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => vars[name] ?? '');
}
