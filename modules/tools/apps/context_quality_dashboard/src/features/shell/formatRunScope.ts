import type { RunScope } from '../../types';

export function formatRunScopeKey(scope: RunScope): string {
  switch (scope.kind) {
    case 'all':
      return 'runStatus.scope.all';
    case 'dimension':
      return `dimension.${scope.dimensionId}`;
    case 'metric':
      return `metric.${scope.metricId}`;
  }
}
