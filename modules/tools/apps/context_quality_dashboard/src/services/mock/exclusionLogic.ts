import type { AppConfigDto, TimeSeries, Equipment, FileRecord  } from '../../types';

export function isSyntheticTimeSeries(ts: TimeSeries): boolean {
  return ts.kind === 'calculated' || ts.kind === 'virtual';
}

export function matchesTagPattern(externalId: string, pattern: string): boolean {
  if (pattern.endsWith('*')) {
    return externalId.startsWith(pattern.slice(0, -1));
  }
  return externalId === pattern;
}

export function isUserExcludedTimeSeries(ts: TimeSeries, config: AppConfigDto): boolean {
  return config.exclusionRules.some(
    (rule) =>
      rule.enabled &&
      rule.kind === 'tag-pattern' &&
      matchesTagPattern(ts.externalId, rule.pattern)
  );
}

export function isUserExcludedEquipment(eq: Equipment, config: AppConfigDto): boolean {
  if (eq.status === 'decommissioned') {
    return config.exclusionRules.some(
      (rule) => rule.enabled && rule.kind === 'lifecycle-status'
    );
  }
  return false;
}

export function isUserExcludedFile(file: FileRecord, config: AppConfigDto): boolean {
  if (file.userExcluded) {
    return true;
  }
  return config.exclusionRules.some(
    (rule) =>
      rule.enabled &&
      rule.kind === 'file-extension' &&
      file.name.toLowerCase().endsWith(rule.pattern.toLowerCase())
  );
}

export function isEligibleTimeSeries(ts: TimeSeries, config: AppConfigDto): boolean {
  if (isSyntheticTimeSeries(ts)) {
    return false;
  }
  return !isUserExcludedTimeSeries(ts, config);
}

export function isEligibleEquipment(eq: Equipment, config: AppConfigDto): boolean {
  return !isUserExcludedEquipment(eq, config);
}

export function isEligibleFile(file: FileRecord, config: AppConfigDto): boolean {
  if (file.isLocationCentric) {
    return false;
  }
  return !isUserExcludedFile(file, config);
}

export function percent(linked: number, total: number): number {
  if (total === 0) {
    return 0;
  }
  return Math.round((linked / total) * 100);
}
