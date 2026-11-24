import type { TierScore, UntieredScoreDto } from '../../types';
import { sectionHeadingSpacedClassName } from '../shell/sectionHeading';

import { formatUntieredReadiness } from './dimensionMetricPresentation';
import type { DimensionViewModel } from './useDimensionViewModel';

type TierQualityTableProps = {
  viewModel: DimensionViewModel;
  tierScores: readonly TierScore[];
  /** Entities with no tier assigned, shown on their own row so they are never dropped or counted as a tier. */
  untiered?: UntieredScoreDto | null;
};

export function TierQualityTable({ viewModel, tierScores, untiered = null }: TierQualityTableProps) {
  return (
    <section aria-label={viewModel.t('dimension.tierQuality')}>
      <h2 className={sectionHeadingSpacedClassName}>{viewModel.t('dimension.tierQuality')}</h2>
      <div className="overflow-x-auto rounded-md border border-border">
        <table className="w-full text-sm">
          <thead className="bg-muted-background text-left text-xs text-muted-foreground">
            <tr>
              <th className="px-3 py-2">{viewModel.t('dimension.tier')}</th>
              <th className="px-3 py-2">{viewModel.t('dimension.quality')}</th>
              <th className="px-3 py-2">{viewModel.t('dimension.target')}</th>
            </tr>
          </thead>
          <tbody>
            {tierScores.map((tierScore) => (
              <tr key={tierScore.tier} className="border-t border-border">
                <td className="px-3 py-2 font-medium">Tier {tierScore.tier}</td>
                <td
                  className={`px-3 py-2 tabular-nums ${
                    tierScore.scored &&
                    tierScore.scorePercent !== null &&
                    tierScore.scorePercent < tierScore.targetPercent
                      ? 'text-destructive-foreground'
                      : ''
                  }`}
                >
                  {tierScore.scorePercent === null ? '—' : `${tierScore.scorePercent}%`}
                </td>
                <td className="px-3 py-2 tabular-nums text-muted-foreground">
                  {tierScore.scored ? `${tierScore.targetPercent}%` : '—'}
                </td>
              </tr>
            ))}
            {untiered === null ? null : (
              <tr className="border-t border-border" data-testid="tier-row-untiered">
                <td className="px-3 py-2 font-medium">{viewModel.t('dimension.tier.untiered')}</td>
                <td className="px-3 py-2 tabular-nums">
                  {untiered.eligibleCount === 0
                    ? '—'
                    : formatUntieredReadiness(untiered.passingCount, untiered.eligibleCount, viewModel.t)}
                </td>
                <td className="px-3 py-2 tabular-nums text-muted-foreground">—</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
