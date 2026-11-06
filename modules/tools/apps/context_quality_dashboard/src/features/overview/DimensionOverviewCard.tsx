import { Badge } from '@cognite/aura/components/badge';
import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { IconChevronRight } from '@cognite/aura/icons';

import type { SnapshotPointDto } from '../../types';

import {
  buildCoverageBreakdownAriaLabel,
  formatCount,
} from './contextualisationCoverageLabels';
import type { DimensionCardCounts } from './dimensionCardCounts';
import { DimensionCardTrendFooter } from './DimensionCardTrendFooter';
import { DimensionPopulationBar } from './DimensionPopulationBar';
import { DimensionPopulationLegend } from './DimensionPopulationLegend';
import type { DimensionPresentation } from './overviewPresentation';

// Annotations are approved or not, rather than contextualised or remaining
const APPROVAL_LABEL_KEYS = {
  contextualised: 'dimension.wording.approvalMix.passing',
  remaining: 'dimension.wording.approvalMix.remaining',
} as const;

type DimensionOverviewCardProps = {
  dimension: DimensionPresentation;
  counts: DimensionCardCounts | undefined;
  trendPoints: SnapshotPointDto[];
  trendLoading: boolean;
  /** When this card was measured, said only when an earlier run than the freshest card's; null otherwise. */
  measuredCaption?: string | null;
  t: (key: string) => string;
  onOpen: () => void;
};

export function DimensionOverviewCard({
  dimension,
  counts,
  trendPoints,
  trendLoading,
  measuredCaption = null,
  t,
  onOpen,
}: DimensionOverviewCardProps) {
  // Annotations are counted, not measured against a population, so the card shows how many there are and no share of them
  const countsOnly = dimension.dimensionId === 'file-annotation';
  const coveragePercentLabel =
    counts?.contextualisedPercent === null || counts === undefined || countsOnly
      ? null
      : interpolate(t('overview.dimensionCard.coveragePercent'), {
          percent: String(counts.contextualisedPercent),
        });

  return (
    <Card
      data-testid={`dimension-card-${dimension.dimensionId}`}
      className="group h-full gap-0 border border-border bg-card-background p-0 shadow-none transition-colors hover:border-border/80"
    >
      <CardContent className="flex h-full w-full flex-col items-stretch gap-3 p-4">
        <div className="flex w-full items-center gap-2">
          <h3 className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">
            {t(dimension.nameKey)}
          </h3>
          {coveragePercentLabel !== null ? (
            <Badge
              variant="sky"
              className="shrink-0 text-xs font-medium tabular-nums"
              data-testid="dimension-card-coverage"
            >
              {coveragePercentLabel}
            </Badge>
          ) : null}
        </div>

        {dimension.enabled && counts !== undefined ? (
          <>
            <p
              className="text-xl font-semibold tabular-nums tracking-tight text-foreground"
              data-testid={countsOnly ? 'dimension-card-count' : undefined}
            >
              {countsOnly
                ? interpolate(
                    t(counts.inScopeCount === 1 ? 'dimension.annotationCount.one' : 'dimension.annotationCount.other'),
                    { count: formatCount(counts.inScopeCount) }
                  )
                : interpolate(t('overview.dimensionCard.population'), {
                    contextualised: formatCount(counts.contextualisedCount),
                    total: formatCount(counts.inScopeCount),
                  })}
            </p>

            <DimensionPopulationBar
              contextualised={counts.contextualisedCount}
              remaining={counts.unresolvedCount}
              ariaLabel={buildCoverageBreakdownAriaLabel(t, {
                contextualised: counts.contextualisedCount,
                remaining: counts.unresolvedCount,
              })}
            />

            <DimensionPopulationLegend
              contextualised={counts.contextualisedCount}
              remaining={counts.unresolvedCount}
              total={counts.inScopeCount}
              labelKeys={countsOnly ? APPROVAL_LABEL_KEYS : undefined}
              t={t}
            />

            {measuredCaption === null ? null : (
              <p className="text-xs text-muted-foreground" data-testid="dimension-card-measured">
                {measuredCaption}
              </p>
            )}

            <DimensionCardTrendFooter
              chartId={dimension.dimensionId}
              points={trendPoints}
              totalIncludingExcluded={counts.inScopeCount}
              isLoading={trendLoading}
              t={t}
            />

            <Button
              variant="ghost"
              size="sm"
              className="mt-auto h-7 w-fit shrink-0 px-0 text-muted-foreground hover:text-foreground"
              data-testid="dimension-card-open"
              onClick={onOpen}
            >
              {t('overview.openDimension')}
              <IconChevronRight className="size-4" />
            </Button>
          </>
        ) : (
          <>
            <p className="text-sm font-medium text-muted-foreground">{t('overview.disabled')}</p>
            <p className="text-xs text-muted-foreground">{t('overview.dimensionUnavailable')}</p>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function interpolate(template: string, vars: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => vars[name] ?? '');
}
