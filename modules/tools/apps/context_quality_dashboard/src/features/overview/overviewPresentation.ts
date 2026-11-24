import type { DimensionCardDto, DimensionId, OverviewDto } from '../../types';

export type CriticalityLevel = 'critical' | 'important' | 'standard';

export type TrendDirection = 'up' | 'down' | 'stable';

/** What the Overview shows for one dimension. */
export type DimensionPresentation = {
  dimensionId: DimensionId;
  nameKey: string;
  enabled: boolean;
  optional: boolean;
  overallPercent: number | null;
  issueCount: number | null;
  /** What the headline metric measured and how much of it passes, when the producer says. */
  eligibleCount?: number;
  passingCount?: number;
  /** When the run this dimension comes from finished: a run can measure some dimensions only. */
  measuredAt?: string;
};

export type OverviewPresentation = {
  dimensions: DimensionPresentation[];
};

function presentDimension(card: DimensionCardDto): DimensionPresentation {
  if (!card.enabled) {
    return {
      dimensionId: card.dimensionId,
      nameKey: card.nameKey,
      enabled: false,
      optional: card.optional,
      overallPercent: null,
      issueCount: null,
    };
  }
  return {
    dimensionId: card.dimensionId,
    nameKey: card.nameKey,
    enabled: true,
    optional: card.optional,
    overallPercent: card.overallPercent,
    issueCount: card.issueCount,
    ...(card.eligibleCount === undefined ? {} : { eligibleCount: card.eligibleCount }),
    ...(card.passingCount === undefined ? {} : { passingCount: card.passingCount }),
    ...(card.measuredAt === undefined ? {} : { measuredAt: card.measuredAt }),
  };
}

export function buildOverviewPresentation(input: { overview: OverviewDto }): OverviewPresentation {
  return { dimensions: input.overview.dimensionCards.map(presentDimension) };
}
