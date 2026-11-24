export function severityLabelKey(severity: string): string {
  switch (severity) {
    case 'critical':
      return 'alerts.severity.critical';
    case 'warning':
      return 'alerts.severity.warning';
    case 'info':
      return 'alerts.severity.informational';
    default:
      return 'alerts.severity.informational';
  }
}

export function severityBadgeVariant(severity: string): 'error' | 'warning' | 'secondary' {
  switch (severity) {
    case 'critical':
      return 'error';
    case 'warning':
      return 'warning';
    default:
      return 'secondary';
  }
}

export function alertTypeLabelKey(type: string): string {
  return `alerts.type.${type}`;
}

export function alertStatusLabelKey(status: string): string {
  return `alerts.status.${status}`;
}
