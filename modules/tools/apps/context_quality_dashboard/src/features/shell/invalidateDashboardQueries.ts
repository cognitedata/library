import type { QueryClient } from '@tanstack/react-query';

export async function invalidateDashboardQueries(queryClient: QueryClient): Promise<void> {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['overview'] }),
    queryClient.invalidateQueries({ queryKey: ['classifications'] }),
    queryClient.invalidateQueries({ queryKey: ['configuredMetrics'] }),
    queryClient.invalidateQueries({ queryKey: ['latestRunConfiguration'] }),
    queryClient.invalidateQueries({ queryKey: ['customQualityMetrics'] }),
    queryClient.invalidateQueries({ queryKey: ['dimension'] }),
    queryClient.invalidateQueries({ queryKey: ['drilldown'] }),
    queryClient.invalidateQueries({ queryKey: ['alerts'] }),
    queryClient.invalidateQueries({ queryKey: ['snapshots'] }),
    queryClient.invalidateQueries({ queryKey: ['configured-metric-trends'] }),
    queryClient.invalidateQueries({ queryKey: ['runStatus'] }),
    queryClient.invalidateQueries({ queryKey: ['runWarnings'] }),
  ]);
}
